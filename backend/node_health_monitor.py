#!/usr/bin/env python3
"""
Node Health Monitor - Track node online/offline status

Monitors peer nodes and marks them as offline if they haven't been seen
for a certain period of time.
"""

import asyncio
import time
from typing import Dict, Callable, Optional
from datetime import datetime, timezone


class NodeHealthMonitor:
    """
    Monitors health of discovered nodes
    
    Features:
    - Tracks last seen time for each peer
    - Marks nodes offline after timeout
    - Callback when node goes offline
    """
    
    def __init__(
        self,
        timeout_seconds: float = 20.0,  # Mark offline after 20 seconds (2-3 missed heartbeats)
        check_interval: float = 5.0,     # Check every 5 seconds
        on_node_offline: Optional[Callable] = None
    ):
        self.timeout_seconds = timeout_seconds
        self.check_interval = check_interval
        self.on_node_offline = on_node_offline
        
        # Track last seen time for each peer
        self.peer_last_seen: Dict[str, float] = {}  # node_id -> timestamp
        self.peer_status: Dict[str, str] = {}       # node_id -> "online" or "offline"
        
        # Background task
        self.monitor_task: Optional[asyncio.Task] = None
        self.running = False
    
    async def start(self):
        """Start health monitoring"""
        self.running = True
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        print(f"💓 Health monitor started (timeout: {self.timeout_seconds}s)")
    
    async def stop(self):
        """Stop health monitoring"""
        self.running = False
        if self.monitor_task:
            self.monitor_task.cancel()
        print("💓 Health monitor stopped")
    
    def update_peer(self, node_id: str):
        """Update last seen time for a peer"""
        current_time = time.time()
        
        # Check if this is a new peer
        is_new_peer = node_id not in self.peer_last_seen
        
        # Update timestamp (this is the "heartbeat")
        self.peer_last_seen[node_id] = current_time
        
        # Update status
        if is_new_peer:
            # New peer discovered - initialize as online
            self.peer_status[node_id] = "online"
            print(f"💓 Heartbeat initialized for new peer: {node_id}")
        elif self.peer_status.get(node_id) != "online":
            # Peer was offline, now back online
            self.peer_status[node_id] = "online"
            print(f"✅ Peer {node_id} reconnected")
    
    def remove_peer(self, node_id: str):
        """Remove peer from monitoring"""
        if node_id in self.peer_last_seen:
            del self.peer_last_seen[node_id]
        if node_id in self.peer_status:
            del self.peer_status[node_id]
    
    def get_peer_status(self, node_id: str) -> str:
        """Get current status of a peer"""
        return self.peer_status.get(node_id, "unknown")
    
    def get_all_statuses(self) -> Dict[str, str]:
        """Get status of all peers"""
        return self.peer_status.copy()
    
    async def _monitor_loop(self):
        """Background loop to check node health"""
        while self.running:
            try:
                await asyncio.sleep(self.check_interval)
                
                current_time = time.time()
                
                # Check each peer
                for node_id, last_seen in list(self.peer_last_seen.items()):
                    time_since_seen = current_time - last_seen
                    
                    # Check if node has timed out
                    if time_since_seen > self.timeout_seconds:
                        previous_status = self.peer_status.get(node_id)
                        
                        if previous_status != "offline":
                            # Node just went offline
                            self.peer_status[node_id] = "offline"
                            print(f"⚠️  Node {node_id} marked offline ({time_since_seen:.1f}s since last seen)")
                            
                            # Callback
                            if self.on_node_offline:
                                await self.on_node_offline(node_id)
            
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"⚠️  Health monitor error: {e}")


# Example usage
async def main():
    """Test health monitor"""
    
    async def node_offline(node_id):
        print(f"Callback: Node {node_id} is now offline!")
    
    monitor = NodeHealthMonitor(
        timeout_seconds=5.0,
        check_interval=1.0,
        on_node_offline=node_offline
    )
    
    await monitor.start()
    
    # Simulate nodes
    monitor.update_peer("node001")
    monitor.update_peer("node002")
    
    print("Waiting for node001 to timeout...")
    
    # Update node002 periodically, but not node001
    for i in range(10):
        await asyncio.sleep(1)
        monitor.update_peer("node002")  # Keep node002 alive
        statuses = monitor.get_all_statuses()
        print(f"Statuses: {statuses}")
    
    await monitor.stop()


if __name__ == "__main__":
    asyncio.run(main())