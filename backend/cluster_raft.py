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

Log replication state (Phase 1+):
  commit_index    — highest log entry confirmed by quorum (cluster-wide)
  last_applied    — highest log entry this node has applied to its DB
  snapshot_index  — last log index covered by an installed snapshot (Phase 3)

Per-follower tracking (Phase 1 init, Phase 2 use):
  next_index[peer]   — next log index to send to that follower
  match_index[peer]  — highest log index confirmed on that follower

Phase 2 additions:
  _advance_commit_index(leader_last_log_index) — quorum commit check
  receive_heartbeat() — now accepts and applies commit_index from leader
  _thread_send_heartbeats() — now includes commit_index in HTTP message
    so followers can advance their apply loop between log proposes
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

        # Raft state persistence
        self._state_file = None
        if data_dir:
            import pathlib
            state_dir = pathlib.Path(data_dir)
            state_dir.mkdir(parents=True, exist_ok=True)
            self._state_file = state_dir / "raft_state.json"

        # ── Core Raft election state (persisted) ──────────────────────
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None

        # ── Log replication state (Phase 1+, persisted) ───────────────
        #
        # commit_index: highest log index confirmed by a quorum. The leader
        #   advances this after receiving ACKs from floor(N/2)+1 nodes.
        #   Followers learn about it via append_entries (Phase 2) and via
        #   commit_index piggy-backed on HTTP heartbeats.
        #
        # last_applied: highest log index this node has applied to its live
        #   tables (File, FileChunk, Chunk, ChunkLocation). Maintained by the
        #   _apply_log_entries background loop in TossItNode.
        #   Invariant: last_applied <= commit_index <= last log index
        #
        # snapshot_index: highest log index covered by the most recent
        #   installed snapshot. Entries at or below this index can be trimmed
        #   from the raft_log table (Phase 3+).
        self.commit_index: int = 0
        self.last_applied: int = 0
        self.snapshot_index: int = 0

        # ── Per-follower tracking (Phase 2) ───────────────────────────
        #
        # next_index[peer]:  next log entry index to send to that follower.
        #   Initialised to commit_index+1 when a peer joins.
        #   Decremented on append_entries rejection (log backfill).
        #
        # match_index[peer]: highest log entry confirmed replicated on that
        #   follower. Used by _advance_commit_index to find the quorum
        #   commit point.
        self.next_index:  Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}

        # Restore persisted state from disk (if any)
        self._load_persisted_state()

        # Known peers
        self.peers: Dict[str, dict] = {}

        # Cached leader info for pre-vote
        self.leader_peer_info: Optional[dict] = None

        # Per-peer liveness — updated by heartbeat thread
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
            f"commit_index={self.commit_index}, "
            f"last_applied={self.last_applied}, "
            f"timeout: {election_timeout_min}-{election_timeout_max}s, "
            f"port: {self.port} TCP+UDP)"
        )

    async def start(self):
        """Start Raft — opens UDP socket and launches heartbeat thread"""
        self.running = True
        self.state = NodeState.FOLLOWER
        self.last_heartbeat = time.time()
        self._main_loop = asyncio.get_running_loop()

        self._udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self._udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except (AttributeError, OSError):
            pass
        self._udp_sock.bind(('0.0.0.0', self.udp_port))
        self._udp_sock.settimeout(1.0)
        print(f"UDP heartbeat socket bound to :{self.udp_port} (shared with HTTP port)")

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
        """Update known peers and maintain per-follower tracking state."""
        self.peers = peers.copy()

        if len(self.peers) > 0:
            self._has_ever_had_peers = True

        # Initialise per-follower tracking for newly discovered peers.
        # next_index starts at commit_index+1 (optimistic: assume follower is
        # up to date). match_index starts at 0 (nothing confirmed yet).
        for node_id in peers:
            if node_id not in self.next_index:
                self.next_index[node_id]  = self.commit_index + 1
                self.match_index[node_id] = 0

        # Clean up tracking for peers that have left the cluster.
        departed = [nid for nid in list(self.next_index) if nid not in peers]
        for node_id in departed:
            del self.next_index[node_id]
            del self.match_index[node_id]

        if (len(self.peers) == 0
                and self.state != NodeState.LEADER
                and not self._has_ever_had_peers):
            print("No peers at startup — becoming leader")
            if self._main_loop and self._main_loop.is_running():
                asyncio.run_coroutine_threadsafe(self._become_leader(), self._main_loop)

    # ================================================================
    #  STATE PERSISTENCE
    # ================================================================

    def _persist_state(self):
        """
        Persist Raft state to disk atomically.

        Persisted fields:
          current_term, voted_for  — election safety
          commit_index             — prevents re-committing on restart
          last_applied             — prevents re-applying on restart
          snapshot_index           — log compaction boundary (Phase 3)
        """
        if not self._state_file:
            return

        try:
            state = {
                "current_term":   self.current_term,
                "voted_for":      self.voted_for,
                "commit_index":   self.commit_index,
                "last_applied":   self.last_applied,
                "snapshot_index": self.snapshot_index,
            }
            tmp = self._state_file.with_suffix('.tmp')
            tmp.write_text(json.dumps(state))
            tmp.rename(self._state_file)
        except Exception as e:
            print(f"Failed to persist Raft state: {e}")

    def _load_persisted_state(self):
        """Load persisted Raft state from disk on startup."""
        if not self._state_file or not self._state_file.exists():
            return

        try:
            state = json.loads(self._state_file.read_text())
            self.current_term   = state.get("current_term", 0)
            self.voted_for      = state.get("voted_for", None)
            self.commit_index   = state.get("commit_index", 0)
            self.last_applied   = state.get("last_applied", 0)
            self.snapshot_index = state.get("snapshot_index", 0)
            print(
                f"Restored Raft state: "
                f"term={self.current_term}, "
                f"voted_for={self.voted_for}, "
                f"commit_index={self.commit_index}, "
                f"last_applied={self.last_applied}"
            )
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
        """
        print(f"Heartbeat thread started (interval={self.heartbeat_interval}s)")

        while self.running:
            try:
                if self.state == NodeState.LEADER:
                    self._thread_send_heartbeats()
                    time.sleep(self.heartbeat_interval)
                else:
                    self._thread_recv_udp_heartbeats()
                    self._thread_check_election()
            except Exception as e:
                print(f"Heartbeat thread error: {e}")
                time.sleep(1.0)

        print(f"Heartbeat thread stopped")

    def _thread_send_heartbeats(self):
        """
        Send UDP + HTTP heartbeats to all peers (leader only).

        Phase 2: commit_index is now included in the HTTP message so
        followers can advance their apply loop even between log proposes.
        The heartbeat thread sends to /api/raft/heartbeat which is kept
        as a backward-compat shim on Phase 2 nodes. Phase 3+ may switch
        this to /api/raft/append_entries directly.
        """
        for node_id, peer_info in list(self.peers.items()):
            peer_ip   = peer_info['ip_address']
            peer_port = peer_info['port']

            # --- UDP heartbeat (fast, timing-critical) ---
            try:
                packet = self._build_udp_heartbeat()
                self._udp_sock.sendto(packet, (peer_ip, peer_port))
            except Exception:
                pass

            # --- HTTP heartbeat (authoritative, state-critical) ---
            # Phase 2: commit_index is piggybacked so followers can
            # advance their apply loop between raft_propose calls.
            try:
                peer_url = f"http://{peer_ip}:{peer_port}/api/raft/heartbeat"

                heartbeat_message = {
                    'leader_id':   self.node_id,
                    'term':        self.current_term,
                    'node_name':   self.node_name,
                    'commit_index': self.commit_index,   # Phase 2 addition
                    'timestamp':   time.time(),
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
                        self.peer_last_seen[node_id] = time.time()
            except Exception:
                pass

        self.last_heartbeat = time.time()

    def _thread_recv_udp_heartbeats(self):
        """
        Receive UDP heartbeats from leader (follower only).

        The socket has a 1s timeout, so this naturally paces the election
        check loop without busy-waiting.
        """
        try:
            data, addr = self._udp_sock.recvfrom(1024)

            if len(data) < UDP_HEADER.size + 4:
                return

            magic_bytes = data[:4]
            magic = struct.unpack('!I', magic_bytes)[0]
            if magic != UDP_MAGIC:
                return

            term, ts = UDP_HEADER.unpack_from(data, 4)
            sender_id = data[4 + UDP_HEADER.size:].decode('utf-8', errors='ignore')

            # Update heartbeat timestamp — prevents spurious elections
            self.last_heartbeat = time.time()

            if sender_id:
                self.peer_last_seen[sender_id] = time.time()

            if term > self.current_term:
                self.current_term = term
                self.leader_id = sender_id
                self._persist_state()

        except socket.timeout:
            pass
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
        self._persist_state()

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

            # Re-initialise per-follower tracking on each new leadership term.
            # next_index resets to commit_index+1 so the leader can discover
            # where each follower's log diverges via the backfill mechanism.
            for node_id in self.peers:
                self.next_index[node_id]  = self.commit_index + 1
                self.match_index[node_id] = 0

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
    #  COMMIT INDEX ADVANCEMENT (Phase 2)
    # ================================================================

    def _advance_commit_index(self, leader_last_log_index: int) -> bool:
        """
        Advance commit_index to the highest index confirmed by a quorum.

        Called by the leader in TossItNode.raft_propose() after collecting
        append_entries ACKs from followers. Also callable after any update
        to match_index (e.g. periodic heartbeat ACKs in Phase 3+).

        Algorithm:
          Collect all confirmed indices: the leader itself always has
          leader_last_log_index, plus each follower's match_index.
          Sort descending. The quorum-th element (0-indexed at quorum-1)
          is the highest index confirmed by at least quorum nodes.
          Only advance if that candidate > current commit_index.

        The Raft term-check safety rule: a leader must not commit entries
        from previous terms by count alone — it must first commit an entry
        from the current term, which then implicitly commits all prior
        entries. For Phase 2, raft_propose only proposes entries in the
        current term and calls _advance_commit_index immediately, so the
        term-check is implicitly satisfied. A full check will be added in
        Phase 3 when log compaction is introduced.

        Returns True if commit_index was advanced, False otherwise.
        """
        total_nodes = 1 + len(self.peers)      # leader + followers
        quorum_size = total_nodes // 2 + 1      # floor(N/2) + 1

        # Build sorted (descending) list of confirmed log indices.
        # leader_last_log_index represents the leader's own position.
        all_confirmed = sorted(
            [leader_last_log_index] + [self.match_index.get(nid, 0) for nid in self.peers],
            reverse=True,
        )

        # The quorum_size-th highest value (0-indexed: quorum_size - 1)
        # is the highest index that at least quorum_size nodes have.
        if len(all_confirmed) < quorum_size:
            return False

        candidate = all_confirmed[quorum_size - 1]

        if candidate > self.commit_index:
            self.commit_index = candidate
            self._persist_state()
            print(f"Raft: commit_index advanced to {self.commit_index} (quorum={quorum_size}/{total_nodes})")
            return True

        return False

    # ================================================================
    #  INBOUND HANDLERS — called from main event loop (FastAPI routes)
    # ================================================================

    async def receive_heartbeat(
        self, leader_id: str, term: int, commit_index: int = 0
    ) -> bool:
        """
        Handle incoming HTTP heartbeat or append_entries from leader.

        Phase 2: now accepts commit_index from the leader so followers can
        advance their apply loop between raft_propose calls (when no new
        entries are being proposed, the heartbeat thread carries the
        current commit_index so followers don't stall).

        Returns True if commit_index was advanced (caller should wake the
        _apply_log_entries event).
        """
        commit_advanced = False

        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._persist_state()
            if self.state != NodeState.FOLLOWER:
                await self._become_follower()

        if term == self.current_term:
            self.leader_id = leader_id
            self.last_heartbeat = time.time()  # Defense-in-depth alongside UDP

            if leader_id in self.peers:
                self.leader_peer_info = self.peers[leader_id].copy()

            if self.state == NodeState.LEADER and leader_id != self.node_id:
                print(f"Another leader detected ({leader_id}), stepping down")
                await self._become_follower()

            # Advance commit_index if leader has committed further than us.
            # This lets followers apply committed entries without waiting for
            # the next raft_propose fan-out to carry a new commit_index.
            if commit_index > self.commit_index:
                self.commit_index = commit_index
                self._persist_state()
                commit_advanced = True

        return commit_advanced

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
            self._persist_state()
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

    def get_commit_index(self) -> int:
        return self.commit_index

    def get_last_applied(self) -> int:
        return self.last_applied

    def get_snapshot_index(self) -> int:
        return self.snapshot_index

    def update_snapshot_index(self, index: int):
        """
        Update snapshot_index and persist. Called by TossItNode after
        _take_snapshot() or _apply_snapshot_to_db() completes so the
        value survives a crash.
        """
        if index > self.snapshot_index:
            self.snapshot_index = index
            self._persist_state()

    def is_peer_alive(self, node_id: str, max_age: float = 30.0) -> bool:
        """Check if a peer has been seen recently by the heartbeat thread."""
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
            print(
                f"State: {raft.get_state()}, "
                f"Leader: {raft.get_leader_id()}, "
                f"commit_index: {raft.get_commit_index()}, "
                f"last_applied: {raft.get_last_applied()}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        await raft.stop()


if __name__ == "__main__":
    asyncio.run(main())
