#!/usr/bin/env python3
"""
TossIt Cluster Raft - Leader Election with Dedicated Heartbeat Thread

The heartbeat runs in its own OS thread, completely independent of the main
asyncio event loop. This means upload I/O can NEVER starve heartbeats,
which was the root cause of spurious elections under heavy load.

Architecture:
  Main thread (asyncio):  uploads, downloads, replication, API handlers
  Heartbeat thread:       sends/monitors heartbeats, triggers elections

The two threads share state through simple attributes (thread-safe under
Python's GIL for simple reads/writes). Async callbacks are dispatched to
the main event loop via asyncio.run_coroutine_threadsafe().
"""

import asyncio
import json
import random
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


class ClusterRaft:
    """
    Raft leader election with a dedicated heartbeat thread.
    
    Key difference from standard asyncio Raft: heartbeats run in their own
    OS thread, so they're never blocked by upload I/O saturating the event loop.
    """
    
    def __init__(
        self,
        node_id: str,
        node_name: str,
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
        
        # SECURITY
        self.identity = identity
        self.trust_store = trust_store
        
        # Async callbacks (run in main event loop)
        self.on_become_leader = on_become_leader
        self.on_lose_leadership = on_lose_leadership
        
        # Raft state (shared between threads — GIL-safe for simple attrs)
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None
        
        # Known peers (node_id -> info dict)
        self.peers: Dict[str, dict] = {}
        
        # Cached leader connection info (survives peer dict modifications)
        self.leader_peer_info: Optional[dict] = None
        
        # Startup guard: don't auto-promote if peers ever existed
        self._has_ever_had_peers = False
        
        # Timing
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.election_timeout = random.uniform(election_timeout_min, election_timeout_max)
        self.heartbeat_interval = heartbeat_interval
        self.last_heartbeat = time.time()
        
        # Thread management
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._main_loop: Optional[asyncio.AbstractEventLoop] = None
        self.running = False
        
        print(f"🗳️  Raft initialized for {node_name} (timeout: {election_timeout_min}-{election_timeout_max}s, dedicated thread)")
    
    async def start(self):
        """Start Raft consensus — launches dedicated heartbeat thread"""
        self.running = True
        self.state = NodeState.FOLLOWER
        self.last_heartbeat = time.time()
        
        # Capture the main event loop so the thread can dispatch callbacks
        self._main_loop = asyncio.get_running_loop()
        
        # Start the heartbeat thread (daemon so it dies with the process)
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_thread_run,
            name="raft-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()
        
        print(f"✓ Raft started — heartbeat thread running independently of event loop")
    
    async def stop(self):
        """Stop Raft consensus"""
        self.running = False
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5.0)
        print("✓ Raft stopped")
    
    def update_peers(self, peers: Dict[str, dict]):
        """Update known peers"""
        self.peers = peers.copy()
        
        if len(self.peers) > 0:
            self._has_ever_had_peers = True
        
        # Auto-promote ONLY at startup with zero peers ever seen
        if (len(self.peers) == 0
                and self.state != NodeState.LEADER
                and not self._has_ever_had_peers):
            print("📊 No peers at startup — becoming leader")
            if self._main_loop and self._main_loop.is_running():
                asyncio.run_coroutine_threadsafe(self._become_leader(), self._main_loop)
    
    # ================================================================
    #  HEARTBEAT THREAD — runs independently of asyncio event loop
    # ================================================================
    
    def _heartbeat_thread_run(self):
        """
        Main loop for the dedicated heartbeat thread.
        
        This thread handles both sides of the heartbeat protocol:
        - Leader: sends heartbeats to all peers every N seconds
        - Follower: monitors last_heartbeat and triggers elections if expired
        
        Uses synchronous urllib (not aiohttp) so it's completely independent
        of the main asyncio event loop. Upload I/O cannot starve this thread.
        """
        print(f"   💓 Heartbeat thread started (interval={self.heartbeat_interval}s)")
        
        while self.running:
            try:
                if self.state == NodeState.LEADER:
                    self._thread_send_heartbeats()
                    time.sleep(self.heartbeat_interval)
                else:
                    # Follower/candidate: check election timeout
                    time.sleep(1.0)
                    self._thread_check_election()
            except Exception as e:
                print(f"⚠️  Heartbeat thread error: {e}")
                time.sleep(1.0)
        
        print(f"   💓 Heartbeat thread stopped")
    
    def _thread_send_heartbeats(self):
        """Send heartbeats to all peers (runs in heartbeat thread)"""
        for node_id, peer_info in list(self.peers.items()):
            try:
                peer_url = f"http://{peer_info['ip_address']}:{peer_info['port']}/api/raft/heartbeat"
                
                heartbeat_message = {
                    'leader_id': self.node_id,
                    'term': self.current_term,
                    'node_name': self.node_name,
                    'timestamp': time.time(),
                }
                
                # Sign if identity available
                if self.identity:
                    payload = self.identity.sign_json(heartbeat_message)
                else:
                    payload = heartbeat_message
                
                data = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(
                    peer_url,
                    data=data,
                    headers={'Content-Type': 'application/json'},
                    method='POST',
                )
                
                with urllib.request.urlopen(req, timeout=3.0) as resp:
                    pass  # 200 = acknowledged
                    
            except Exception:
                pass  # Peer unreachable — they'll timeout and trigger election
        
        self.last_heartbeat = time.time()
    
    def _thread_check_election(self):
        """Check if election timeout has expired (runs in heartbeat thread)"""
        elapsed = time.time() - self.last_heartbeat
        
        if elapsed <= self.election_timeout:
            return  # Timer hasn't expired
        
        # Timer expired — pre-vote: ping leader before disrupting the cluster
        if self._thread_leader_is_reachable():
            self.last_heartbeat = time.time()
            return  # Leader alive, just slow on formal heartbeats
        
        # Leader genuinely unreachable — start election
        print(f"⏰ Election timeout ({self.election_timeout:.1f}s) — leader unreachable, starting election")
        self._thread_start_election()
        
        # Reset
        self.election_timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
        self.last_heartbeat = time.time()
    
    def _thread_leader_is_reachable(self) -> bool:
        """
        Pre-vote: ping the leader's lightweight /api/raft/ping endpoint.
        
        This endpoint does ZERO disk I/O (unlike /api/health which calls
        _update_capacity). Returns 200 instantly even under max upload load.
        Falls back to /api/health if ping isn't available.
        """
        if not self.leader_id:
            return False
        
        peer_info = self.leader_peer_info
        if not peer_info and self.leader_id in self.peers:
            peer_info = self.peers[self.leader_id]
        if not peer_info:
            return False
        
        base = f"http://{peer_info['ip_address']}:{peer_info['port']}"
        
        # Try lightweight ping first, fall back to health
        for endpoint in ["/api/raft/ping", "/api/health"]:
            try:
                req = urllib.request.Request(f"{base}{endpoint}", method='GET')
                with urllib.request.urlopen(req, timeout=5.0) as resp:
                    if resp.status == 200:
                        return True
            except Exception:
                continue
        
        return False
    
    def _thread_start_election(self):
        """Run a leader election (from heartbeat thread)"""
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.leader_id = None
        
        print(f"📢 Starting election for term {self.current_term}")
        
        votes_received = 1  # Self-vote
        
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
                    peer_url,
                    data=data,
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
            print(f"✅ Won election with {votes_received}/{total_nodes} votes")
            # Dispatch async callback to main event loop
            if self._main_loop and self._main_loop.is_running():
                asyncio.run_coroutine_threadsafe(self._become_leader(), self._main_loop)
            else:
                self.state = NodeState.LEADER
                self.leader_id = self.node_id
        else:
            print(f"❌ Lost election with {votes_received}/{total_nodes} votes")
            self.state = NodeState.FOLLOWER
            self.last_heartbeat = time.time()
    
    # ================================================================
    #  STATE TRANSITIONS — run in main event loop (for async callbacks)
    # ================================================================
    
    async def _become_leader(self):
        """Transition to leader state"""
        was_leader = self.state == NodeState.LEADER
        
        self.state = NodeState.LEADER
        self.leader_id = self.node_id
        
        if not was_leader:
            print(f"👑 Became leader for term {self.current_term}")
            # Heartbeat sending is handled by the dedicated thread —
            # no need to create an asyncio task for it.
            if self.on_become_leader:
                await self.on_become_leader()
    
    async def _become_follower(self):
        """Transition to follower state"""
        was_leader = self.state == NodeState.LEADER
        
        self.state = NodeState.FOLLOWER
        self.last_heartbeat = time.time()
        
        if was_leader:
            print(f"📉 Stepped down as leader")
            if self.on_lose_leadership:
                await self.on_lose_leadership()
    
    # ================================================================
    #  INBOUND HANDLERS — called from main event loop (FastAPI routes)
    # ================================================================
    
    async def receive_heartbeat(self, leader_id: str, term: int):
        """Handle incoming heartbeat from leader"""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            if self.state != NodeState.FOLLOWER:
                await self._become_follower()
        
        if term == self.current_term:
            self.leader_id = leader_id
            self.last_heartbeat = time.time()
            
            # Cache leader connection info for pre-vote checks
            if leader_id in self.peers:
                self.leader_peer_info = self.peers[leader_id].copy()
            
            if self.state == NodeState.LEADER and leader_id != self.node_id:
                print(f"⚠️  Another leader detected ({leader_id}), stepping down")
                await self._become_follower()
    
    async def request_vote(self, candidate_id: str, term: int) -> bool:
        """Handle incoming vote request from candidate"""
        if term < self.current_term:
            return False
        
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            await self._become_follower()
        
        if self.voted_for is None or self.voted_for == candidate_id:
            self.voted_for = candidate_id
            self.last_heartbeat = time.time()
            print(f"🗳️  Voted for {candidate_id} in term {term}")
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


# Example usage
async def main():
    async def became_leader():
        print("🎉 Callback: I am now the leader!")
    
    async def lost_leadership():
        print("😔 Callback: I am no longer the leader")
    
    raft = ClusterRaft(
        node_id="node001",
        node_name="test-node",
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
