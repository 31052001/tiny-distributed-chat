import socket
import threading
import time
from typing import Dict, Tuple, Set, Optional

# ---------------- CONFIG ----------------
UDP_PORT = 12000
TCP_PORT = 12001

HEARTBEAT_INTERVAL = 2.0
PEER_TIMEOUT = 8.0

ACK_TIMEOUT = 1.5
RETRANSMIT_INTERVAL = 1.0

# ---------------- STATE ----------------
# peer_ip -> (peer_id, last_seen_ts)
PEERS: Dict[str, Tuple[int, float]] = {}

MY_ID: int = 0
MY_IP: str = ""
CURRENT_LEADER_ID: int = 0
IS_LEADER: bool = False

# Bully election helpers
ELECTION_IN_PROGRESS = False
LAST_OK_TS = 0.0
WAIT_COORDINATOR_UNTIL = 0.0

# Total order + reliability
SEQ: int = 0  # only used by leader
# seq -> (sender_id, text)
LEADER_LOG: Dict[int, Tuple[int, str]] = {}
# seq -> set(peer_id) who acked
ACKS: Dict[int, Set[int]] = {}
# peer side:
EXPECTED_SEQ = 1
BUFFERED: Dict[int, Tuple[int, str]] = {}

# ---------------- UTIL ----------------
def now() -> float:
    return time.time()

def get_local_ip() -> str:
    """
    Reliable-ish local IP detection (works on hotspot/LAN).
    Doesn't send packets; just asks OS which interface would be used.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def broadcast_addr_for_hotspot() -> str:
    """
    For many hotspot/LAN setups, 255.255.255.255 is sufficient.
    If your hotspot blocks global broadcast, you can switch to subnet broadcast manually.
    """
    return "255.255.255.255"

def peer_count() -> int:
    return len(PEERS)

def all_known_ids() -> Set[int]:
    ids = {MY_ID}
    for _, (pid, _) in PEERS.items():
        ids.add(pid)
    return ids

def leader_ip() -> Optional[str]:
    if IS_LEADER:
        return MY_IP
    for ip, (pid, _) in PEERS.items():
        if pid == CURRENT_LEADER_ID:
            return ip
    return None

# ---------------- UDP (DISCOVERY + HEARTBEAT + ELECTION) ----------------
def udp_send(message: str, target_ip: str, port: int = UDP_PORT) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.sendto(message.encode("utf-8"), (target_ip, port))
    finally:
        s.close()

def udp_broadcast(message: str) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.sendto(message.encode("utf-8"), (broadcast_addr_for_hotspot(), UDP_PORT))
    finally:
        s.close()

def udp_listener() -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", UDP_PORT))

    while True:
        try:
            data, addr = s.recvfrom(4096)
            msg = data.decode("utf-8", errors="ignore")
            src_ip = addr[0]
            handle_udp(msg, src_ip)
        except Exception:
            time.sleep(0.05)

def handle_udp(msg: str, src_ip: str) -> None:
    global CURRENT_LEADER_ID, IS_LEADER, ELECTION_IN_PROGRESS, LAST_OK_TS, WAIT_COORDINATOR_UNTIL

    if src_ip == MY_IP:
        return

    parts = msg.split(":")
    if not parts:
        return

    kind = parts[0]

    # HELLO:<id>
    if kind == "HELLO" and len(parts) >= 2:
        pid = int(parts[1])
        is_new = src_ip not in PEERS
        PEERS[src_ip] = (pid, now())
        if is_new:
            print(f"\n[DISCOVERY] Found peer {pid} @ {src_ip}. Peers={peer_count()}")
        return

    # HEARTBEAT:<id>
    if kind == "HEARTBEAT" and len(parts) >= 2:
        pid = int(parts[1])
        PEERS[src_ip] = (pid, now())
        return

    # ELECTION:<sender_id>
    if kind == "ELECTION" and len(parts) >= 2:
        sender_id = int(parts[1])
        # if my id is higher, respond OK and start my own election (bully)
        if MY_ID > sender_id:
            udp_send(f"OK:{MY_ID}", src_ip)
            # start/continue election if not already
            if not ELECTION_IN_PROGRESS:
                start_election()
        return

    # OK:<responder_id>
    if kind == "OK" and len(parts) >= 2:
        # someone higher is alive
        LAST_OK_TS = now()
        # wait for coordinator announcement
        WAIT_COORDINATOR_UNTIL = now() + 3.0
        return

    # COORDINATOR:<leader_id>
    if kind == "COORDINATOR" and len(parts) >= 2:
        leader_id = int(parts[1])
        CURRENT_LEADER_ID = leader_id
        IS_LEADER = (leader_id == MY_ID)
        ELECTION_IN_PROGRESS = False
        if IS_LEADER:
            print(f"\n[ELECTION] *** I AM LEADER (ID {MY_ID}) ***")
        else:
            print(f"\n[ELECTION] New leader: ID {leader_id}")
        return

# ---------------- HEARTBEAT + FAILURE DETECTION ----------------
def heartbeat_loop() -> None:
    # send hello once quickly, then heartbeats forever
    udp_broadcast(f"HELLO:{MY_ID}")
    while True:
        udp_broadcast(f"HEARTBEAT:{MY_ID}")
        time.sleep(HEARTBEAT_INTERVAL)

def failure_detector_loop() -> None:
    global CURRENT_LEADER_ID
    while True:
        t = now()
        removed = []
        for ip, (pid, last_seen) in list(PEERS.items()):
            if t - last_seen > PEER_TIMEOUT:
                removed.append((ip, pid))
        for ip, pid in removed:
            PEERS.pop(ip, None)
            print(f"\n[FAULT] Peer {pid} @ {ip} timed out. Peers={peer_count()}")
            if pid == CURRENT_LEADER_ID:
                print("[FAULT] Leader timed out -> starting election.")
                start_election()
        time.sleep(1.0)

# ---------------- BULLY ELECTION ----------------
def start_election() -> None:
    """
    Bully: send ELECTION to all peers with higher ID.
    If no OK arrives within short time, declare self coordinator.
    """
    global ELECTION_IN_PROGRESS, LAST_OK_TS, WAIT_COORDINATOR_UNTIL

    ELECTION_IN_PROGRESS = True
    LAST_OK_TS = 0.0
    WAIT_COORDINATOR_UNTIL = now() + 2.5

    higher_peers = [(ip, pid) for ip, (pid, _) in PEERS.items() if pid > MY_ID]

    if not higher_peers:
        become_coordinator()
        return

    for ip, _pid in higher_peers:
        udp_send(f"ELECTION:{MY_ID}", ip)

    threading.Thread(target=_election_wait_loop, daemon=True).start()

def _election_wait_loop() -> None:
    global ELECTION_IN_PROGRESS
    # wait for OK
    time.sleep(2.0)
    if LAST_OK_TS == 0.0:
        become_coordinator()
        return
    # OK received -> wait for coordinator
    time.sleep(2.5)
    if ELECTION_IN_PROGRESS and now() > WAIT_COORDINATOR_UNTIL:
        # no coordinator came, restart election
        start_election()

def become_coordinator() -> None:
    global CURRENT_LEADER_ID, IS_LEADER, ELECTION_IN_PROGRESS
    CURRENT_LEADER_ID = MY_ID
    IS_LEADER = True
    ELECTION_IN_PROGRESS = False
    udp_broadcast(f"COORDINATOR:{MY_ID}")
    print(f"\n[ELECTION] *** I AM LEADER (ID {MY_ID}) ***")

# ---------------- TCP (CHAT DATA) ----------------
def tcp_listener() -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((MY_IP, TCP_PORT))
    server.listen(20)
    print(f"[TCP] Listening on {MY_IP}:{TCP_PORT}")

    while True:
        try:
            conn, _ = server.accept()
            data = conn.recv(8192)
            if data:
                handle_tcp(data.decode("utf-8", errors="ignore"))
            conn.close()
        except Exception:
            time.sleep(0.05)

def tcp_send(ip: str, payload: str) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1.5)
    try:
        s.connect((ip, TCP_PORT))
        s.sendall(payload.encode("utf-8"))
    finally:
        try:
            s.close()
        except Exception:
            pass

# Client -> Leader: CHAT:<sender_id>:<text>
# Leader -> All: ORDERED:<seq>:<sender_id>:<text>
# Peer -> Leader: ACK:<seq>:<peer_id>
def handle_tcp(msg: str) -> None:
    global SEQ, LEADER_LOG, ACKS, EXPECTED_SEQ

    if msg.startswith("CHAT:"):
        if not IS_LEADER:
            return
        _, sender_id_str, text = msg.split(":", 2)
        sender_id = int(sender_id_str)

        SEQ += 1
        seq = SEQ
        LEADER_LOG[seq] = (sender_id, text)

        # init ack set: leader counts itself as acked
        ACKS[seq] = {MY_ID}

        ordered_payload = f"ORDERED:{seq}:{sender_id}:{text}"
        leader_multicast(ordered_payload)
        return

    if msg.startswith("ORDERED:"):
        _, seq_str, sender_id_str, text = msg.split(":", 3)
        seq = int(seq_str)
        sender_id = int(sender_id_str)

        # buffer & deliver in order
        BUFFERED[seq] = (sender_id, text)
        deliver_in_order()

        # ack back to leader
        lip = leader_ip()
        if lip:
            tcp_send(lip, f"ACK:{seq}:{MY_ID}")
        return

    if msg.startswith("ACK:"):
        if not IS_LEADER:
            return
        _, seq_str, peer_id_str = msg.split(":", 2)
        seq = int(seq_str)
        peer_id = int(peer_id_str)

        if seq in ACKS:
            ACKS[seq].add(peer_id)
        return

def deliver_in_order() -> None:
    global EXPECTED_SEQ
    while EXPECTED_SEQ in BUFFERED:
        sender_id, text = BUFFERED.pop(EXPECTED_SEQ)
        print(f"\n[CHAT] (Seq #{EXPECTED_SEQ} from {sender_id}): {text}")
        EXPECTED_SEQ += 1

def leader_multicast(payload: str) -> None:
    # send to all peers + self-process
    for ip in list(PEERS.keys()):
        try:
            tcp_send(ip, payload)
        except Exception:
            pass
    handle_tcp(payload)  # leader delivers to itself + acks itself

def retransmit_loop() -> None:
    """
    Leader retransmits not-fully-acked messages periodically.
    """
    while True:
        if IS_LEADER:
            # expected acks = all known peers + leader
            expected = all_known_ids()
            for seq, (sender_id, text) in list(LEADER_LOG.items()):
                got = ACKS.get(seq, {MY_ID})
                missing = expected - got
                if missing:
                    # resend ordered message to missing peer IPs (if known)
                    payload = f"ORDERED:{seq}:{sender_id}:{text}"
                    for ip, (pid, _) in list(PEERS.items()):
                        if pid in missing:
                            try:
                                tcp_send(ip, payload)
                            except Exception:
                                pass
        time.sleep(RETRANSMIT_INTERVAL)

# ---------------- USER SEND ----------------
def send_chat(text: str) -> None:
    lip = leader_ip()
    if not lip:
        print("[ERROR] No leader known. Starting election...")
        start_election()
        return
    try:
        tcp_send(lip, f"CHAT:{MY_ID}:{text}")
    except Exception as e:
        print(f"[ERROR] Failed to send to leader: {e}")
        start_election()

# ---------------- MAIN ----------------
def main() -> None:
    global MY_ID, MY_IP

    MY_ID = int(input("Enter your unique peer ID (e.g., 10, 20, 30): ").strip())
    MY_IP = get_local_ip()
    print(f"[BOOT] Peer {MY_ID} on IP {MY_IP}")
    print(f"[BOOT] UDP_PORT={UDP_PORT}, TCP_PORT={TCP_PORT}")

    threading.Thread(target=udp_listener, daemon=True).start()
    threading.Thread(target=tcp_listener, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=failure_detector_loop, daemon=True).start()
    threading.Thread(target=retransmit_loop, daemon=True).start()

    # give discovery a moment, then election
    time.sleep(2.5)
    start_election()

    while True:
        try:
            text = input(f"[{MY_ID}] > ").strip()
            if text:
                send_chat(text)
        except (EOFError, KeyboardInterrupt):
            print("\n[EXIT] Bye.")
            break
        except Exception:
            time.sleep(0.1)

if __name__ == "__main__":
    main()
