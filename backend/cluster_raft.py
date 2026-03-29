#!/usr/bin/env python3
"""
TossIt Cluster Raft - Leader Election with UDP Heartbeats

Architecture:
  Main thread (asyncio):  uploads, downloads, replication, API handlers
  Heartbeat thread:       UDP send/recv + HTTP send, elections

Heartbeats use TWO channels:
  UDP (fast, timing):  Small datagram sent/received in heartbeat thread.
                       Resets election timer. No event loop involvement.
  HTTP (authoritative): Signed POST for Raft term/state transitions.
                       Processed by FastAPI when event loop is free.

The election timer ONLY checks the UDP channel. Since UDP send+recv both
happen in the heartbeat thread, upload I/O can never cause false elections.

Port usage:
  Both HTTP and UDP heartbeats use the same port number. TCP and UDP are
  separate protocols at the OS level — they do not compete for the same
  socket. This means a node only ever needs one port configured, exposed
  in Docker, or opened in a firewall.
"""

import asyncio
import json
import random
import socket
import struct
import threading
import time
import urllib.request
import urllib.error
from typing import Optional, Dict, Callable
from enum import Enum


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


# UDP heartbeat packet format:
#   4 bytes: magic (0x54495342 = "TISB")
#   4 bytes: term (uint32)
#   8 bytes: timestamp (double)
#   remaining: node_id (utf-8 string)
UDP_MAGIC = 0x54495342
UDP_HEADER = struct.Struct('!Id')  # network byte order: uint32 + double


class ClusterRaft:
    """
    Raft leader election with UDP heartbeats in a dedicated thread.

    Both heartbeat sending AND receiving happen in their own OS thread
    via raw UDP sockets. The main asyncio event loop is never involved
    in timing-critical heartbeat detection.

    HTTP and UDP both bind to the same port number. The OS routes them
    independently — no port offset required.
    """

    def __init__(
        self,
        node_id: str,
        node_name: str,
        port: int = 8000,
        data_dir: Optional[str] = None,
        identity=None,
        trust_store=None,
        on_become_leader: Optional[Callable] = None,
        on_lose_leadership: Optional[Callable] = None,
        election_timeout_min: float = 15.0,
        election_timeout_max: float = 25.0,
        heartbeat_interval: float = 2.0,
    ):
        self.node_id = node_id
        self.node_name = node_name
        self.port = port
        # UDP and HTTP share the same port number — no offset needed.
        # TCP and UDP are distinct protocols at the kernel level.
        self.udp_port = port

        # SECURITY
        self.identity = identity
        self.trust_store = trust_store

        # Async callbacks (dispatched to main event loop)
        self.on_become_leader = on_become_leader
        self.on_lose_leadership = on_lose_leadership

        # Raft state persistence — survive crashes without violating safety.
        # Without this, a restarted node could vote twice in the same term.
        self._state_file = None
        if data_dir:
            import pathlib
            state_dir = pathlib.Path(data_dir)
            state_dir.mkdir(parents=True, exist_ok=True)
            self._state_file = state_dir / "raft_state.json"

        # Raft state (shared between threads — GIL-safe for simple attrs)
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None

        # Restore persisted state from disk (if any)
        self._load_persisted_state()

        # Known peers
        self.peers: Dict[str, dict] = {}

        # Cached leader info for pre-vote
        self.leader_peer_info: Optional[dict] = None

        # Per-peer liveness — updated by heartbeat thread when a peer
        # responds (HTTP 200) or sends us data (UDP). Used by the health
        # monitor to avoid false "offline" marks during heavy I/O.
        self.peer_last_seen: Dict[str, float] = {}

        self._has_ever_had_peers = False

        # Timing
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.election_timeout = random.uniform(election_timeout_min, election_timeout_max)
        self.heartbeat_interval = heartbeat_interval
        self.last_heartbeat = time.time()

        # UDP socket (created in start())
        self._udp_sock: Optional[socket.socket] = None

        # Thread management
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self.running = False

        print(
            f"Raft initialized for {node_name} "
            f"(term={self.current_term}, "
            f"timeout: {election_timeout_min}-{election_timeout_max}s, "
            f"port: {self.port} TCP+UDP)"
        )

    async def start(self):
        """Start Raft — opens UDP socket and launches heartbeat thread"""
        self.running = True
        self.state = NodeState.FOLLOWER
        self.last_heartbeat = time.time()
        self._main_loop = asyncio.get_running_loop()

        # Open UDP socket for heartbeats.
        # SO_REUSEPORT lets the UDP socket bind to the same port number
        # that uvicorn's TCP socket also uses. The kernel demultiplexes by
        # protocol — they never interfere with each other.
        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            # SO_REUSEPORT is available on Linux and macOS but not Windows.
            self._udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            # Windows or a kernel build without SO_REUSEPORT — fall back
            # gracefully. SO_REUSEADDR alone is sufficient on Windows
            # because it already allows port reuse across protocols there.
            pass
        self._udp_sock.bind(('0.0.0.0', self.udp_port))
        self._udp_sock.settimeout(1.0)  # 1s recv timeout for clean shutdown
        print(f"UDP heartbeat socket bound to :{self.udp_port} (shared with HTTP port)")

        # Launch heartbeat thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_thread_run,
            name="raft-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()

        print(f"✓ Raft started — heartbeat thread with UDP (fully independent of event loop)")

    async def stop(self):
        """Stop Raft"""
        self.running = False
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5.0)
        if self._udp_sock:
            self._udp_sock.close()
        print("✓ Raft stopped")

    def update_peers(self, peers: Dict[str, dict]):
        """Update known peers"""
        self.peers = peers.copy()

        if len(self.peers) > 0:
            self._has_ever_had_peers = True

        if (len(self.peers) == 0
                and self.state != NodeState.LEADER
                and not self._has_ever_had_peers):
            print("No peers at startup — becoming leader")
            if self._main_loop and self._main_loop.is_running():
                asyncio.run_coroutine_threadsafe(self._become_leader(), self._main_loop)

    # ================================================================
    #  STATE PERSISTENCE — survive crashes without safety violations
    # ================================================================

    def _persist_state(self):
        """
        Persist current_term and voted_for to disk.

        Called every time either value changes. This is the minimum state
        needed for Raft safety: without it, a restarted node could vote
        twice in the same term, violating the one-vote-per-term guarantee.

        The write is ~50 bytes to a small JSON file. Even on a busy disk
        this completes in <1ms. It runs in the heartbeat thread so it
        doesn't touch the event loop.
        """
        if not self._state_file:
            return

        try:
            state = {
                "current_term": self.current_term,
                "voted_for": self.voted_for,
            }
            # Atomic write: write to temp file, then rename.
            # Prevents corrupt state if crash occurs mid-write.
            tmp = self._state_file.with_suffix('.tmp')
            tmp.write_text(json.dumps(state))
            tmp.rename(self._state_file)
        except Exception as e:
            print(f"Failed to persist Raft state: {e}")

    def _load_persisted_state(self):
        """Load persisted term/vote from disk on startup."""
        if not self._state_file or not self._state_file.exists():
            return

        try:
            state = json.loads(self._state_file.read_text())
            self.current_term = state.get("current_term", 0)
            self.voted_for = state.get("voted_for", None)
            print(f"Restored Raft state: term={self.current_term}, voted_for={self.voted_for}")
        except Exception as e:
            print(f"Failed to load Raft state (starting fresh): {e}")

    # ================================================================
    #  HEARTBEAT THREAD — fully independent of asyncio event loop
    # ================================================================

    def _heartbeat_thread_run(self):
        """
        Dedicated heartbeat thread.

        Leader mode:  send UDP + HTTP heartbeats, sleep interval
        Follower mode: recv UDP heartbeats, check election timeout

        Both send and receive use raw sockets / urllib — zero event loop.
        """
        print(f"Heartbeat thread started (interval={self.heartbeat_interval}s)")

        while self.running:
            try:
                if self.state == NodeState.LEADER:
                    self._thread_send_heartbeats()
                    time.sleep(self.heartbeat_interval)
                else:
                    # Follower: try to receive UDP heartbeats
                    self._thread_recv_udp_heartbeats()
                    self._thread_check_election()
            except Exception as e:
                print(f"Heartbeat thread error: {e}")
                time.sleep(1.0)

        print(f"Heartbeat thread stopped")

    def _thread_send_heartbeats(self):
        """Send UDP + HTTP heartbeats to all peers (leader only)"""
        for node_id, peer_info in list(self.peers.items()):
            peer_ip   = peer_info['ip_address']
            peer_port = peer_info['port']
            # UDP heartbeat goes to the same port number as HTTP.
            # The peer's kernel routes it to the UDP socket automatically.
            peer_udp_port = peer_port

            # --- UDP heartbeat (fast, timing-critical) ---
            try:
                packet = self._build_udp_heartbeat()
                self._udp_sock.sendto(packet, (peer_ip, peer_udp_port))
            except Exception:
                pass

            # --- HTTP heartbeat (authoritative, state-critical) ---
            try:
                peer_url = f"http://{peer_ip}:{peer_port}/api/raft/heartbeat"

                heartbeat_message = {
                    'leader_id': self.node_id,
                    'term': self.current_term,
                    'node_name': self.node_name,
                    'timestamp': time.time(),
                }

                if self.identity:
                    payload = self.identity.sign_json(heartbeat_message)
                else:
                    payload = heartbeat_message

                data = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(
                    peer_url, data=data,
                    headers={'Content-Type': 'application/json'},
                    method='POST',
                )

                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    if resp.status == 200:
                        # Peer confirmed alive — update liveness tracker
                        self.peer_last_seen[node_id] = time.time()
            except Exception:
                pass

        self.last_heartbeat = time.time()

    def _thread_recv_udp_heartbeats(self):
        """
        Receive UDP heartbeats from leader (follower only).

        The socket has a 1s timeout, so this naturally paces the election
        check loop without busy-waiting. When a heartbeat arrives, we
        update last_heartbeat directly — no event loop.
        """
        try:
            data, addr = self._udp_sock.recvfrom(1024)

            if len(data) < UDP_HEADER.size + 4:
                return  # Runt packet

            # Parse header
            magic_bytes = data[:4]
            magic = struct.unpack('!I', magic_bytes)[0]
            if magic != UDP_MAGIC:
                return  # Not our packet

            term, ts = UDP_HEADER.unpack_from(data, 4)
            sender_id = data[4 + UDP_HEADER.size:].decode('utf-8', errors='ignore')

            # Update heartbeat timestamp — this is the critical line that
            # prevents elections. It runs in THIS thread, not the event loop.
            self.last_heartbeat = time.time()

            # Track peer liveness
            if sender_id:
                self.peer_last_seen[sender_id] = time.time()

            # Update term if higher (leader changed while we were busy)
            if term > self.current_term:
                self.current_term = term
                self.leader_id = sender_id
                self._persist_state()

        except socket.timeout:
            pass  # No UDP heartbeat received — election check will handle it
        except Exception:
            pass

    def _build_udp_heartbeat(self) -> bytes:
        """Build a compact UDP heartbeat packet"""
        magic = struct.pack('!I', UDP_MAGIC)
        header = UDP_HEADER.pack(self.current_term, time.time())
        node_id_bytes = self.node_id.encode('utf-8')
        return magic + header + node_id_bytes

    def _thread_check_election(self):
        """Check election timeout (runs in heartbeat thread)"""
        elapsed = time.time() - self.last_heartbeat

        if elapsed <= self.election_timeout:
            return

        # No UDP heartbeat for election_timeout seconds.
        # Since UDP bypasses the event loop entirely, this means the leader
        # is genuinely unreachable — not just busy with uploads.
        print(f"Election timeout ({self.election_timeout:.1f}s) — no UDP heartbeat, starting election")
        self._thread_start_election()

        self.election_timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
        self.last_heartbeat = time.time()

    def _thread_start_election(self):
        """Run leader election (from heartbeat thread)"""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        self._persist_state()  # Persist before requesting votes

        print(f"Starting election for term {self.current_term}")

        votes_received = 1

        for node_id, peer_info in list(self.peers.items()):
            try:
                peer_url = f"http://{peer_info['ip_address']}:{peer_info['port']}/api/raft/vote"

                vote_message = {
                    'candidate_id': self.node_id,
                    'term': self.current_term,
                    'node_name': self.node_name,
                    'timestamp': time.time(),
                }

                if self.identity:
                    payload = self.identity.sign_json(vote_message)
                else:
                    payload = vote_message

                data = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(
                    peer_url, data=data,
                    headers={'Content-Type': 'application/json'},
                    method='POST',
                )

                with urllib.request.urlopen(req, timeout=2.0) as resp:
                    if resp.status == 200:
                        result = json.loads(resp.read())
                        if result.get('vote_granted'):
                            votes_received += 1
            except Exception:
                pass

        total_nodes = 1 + len(self.peers)

        if votes_received > total_nodes / 2:
            print(f"Won election with {votes_received}/{total_nodes} votes")
            if self._main_loop and self._main_loop.is_running():
                asyncio.run_coroutine_threadsafe(self._become_leader(), self._main_loop)
            else:
                self.state = NodeState.LEADER
                self.leader_id = self.node_id
        else:
            print(f"Lost election with {votes_received}/{total_nodes} votes")
            self.state = NodeState.FOLLOWER
            self.last_heartbeat = time.time()

    # ================================================================
    #  STATE TRANSITIONS — dispatched to main event loop
    # ================================================================

    async def _become_leader(self):
        was_leader = self.state == NodeState.LEADER
        self.state = NodeState.LEADER
        self.leader_id = self.node_id

        if not was_leader:
            print(f"Became leader for term {self.current_term}")
            if self.on_become_leader:
                await self.on_become_leader()

    async def _become_follower(self):
        was_leader = self.state == NodeState.LEADER
        self.state = NodeState.FOLLOWER
        self.last_heartbeat = time.time()

        if was_leader:
            print(f"Stepped down as leader")
            if self.on_lose_leadership:
                await self.on_lose_leadership()

    # ================================================================
    #  INBOUND HANDLERS — called from main event loop (FastAPI routes)
    # ================================================================

    async def receive_heartbeat(self, leader_id: str, term: int):
        """
        Handle incoming HTTP heartbeat from leader.

        This handles Raft state logic (term updates, step-downs).
        The timing-critical last_heartbeat update is handled by UDP,
        but we also update it here as defense-in-depth.
        """
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist_state()
            if self.state != NodeState.FOLLOWER:
                await self._become_follower()

        if term == self.current_term:
            self.leader_id = leader_id
            self.last_heartbeat = time.time()  # Defense-in-depth

            if leader_id in self.peers:
                self.leader_peer_info = self.peers[leader_id].copy()

            if self.state == NodeState.LEADER and leader_id != self.node_id:
                print(f"Another leader detected ({leader_id}), stepping down")
                await self._become_follower()

    async def request_vote(self, candidate_id: str, term: int) -> bool:
        if term < self.current_term:
            return False

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist_state()
            await self._become_follower()

        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            self.last_heartbeat = time.time()
            self._persist_state()  # Persist vote before confirming
            print(f"Voted for {candidate_id} in term {term}")
            return True

        return False

    # ================================================================
    #  PUBLIC API
    # ================================================================

    def is_leader(self) -> bool:
        return self.state == NodeState.LEADER

    def get_leader_id(self) -> Optional[str]:
        return self.leader_id

    def get_state(self) -> str:
        return self.state.value

    def is_peer_alive(self, node_id: str, max_age: float = 30.0) -> bool:
        """
        Check if a peer has been seen recently by the heartbeat thread.

        Used by the health monitor to avoid marking nodes offline when
        the main event loop is too busy to process health check responses.
        If the heartbeat thread has communicated with the peer recently,
        the peer is definitely alive.
        """
        last_seen = self.peer_last_seen.get(node_id, 0)
        return (time.time() - last_seen) < max_age


# Example usage
async def main():
    async def became_leader():
        print("I am now the leader!")

    async def lost_leadership():
        print("I am no longer the leader")

    raft = ClusterRaft(
        node_id="node001",
        node_name="test-node",
        port=8000,
        on_become_leader=became_leader,
        on_lose_leadership=lost_leadership,
    )

    await raft.start()

    try:
        while True:
            await asyncio.sleep(2)
            print(f"State: {raft.get_state()}, Leader: {raft.get_leader_id()}")
    except KeyboardInterrupt:
        pass
    finally:
        await raft.stop()


if __name__ == "__main__":
    asyncio.run(main())
