"""
Registry Client for TossIt Nodes
Handles heartbeat sending to TossIt registry service
"""

import asyncio
import aiohttp
import logging
from typing import Optional
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


class RegistryClient:
    """
    Handles communication with TossIt Registry Service
    
    When this node is the leader, sends periodic heartbeats to registry
    Registry uses these heartbeats to redirect users to the current leader
    """
    
    def __init__(self, registry_url: str, cluster_id: str, node_name: str, port: int):
        """
        Initialize registry client
        
        Args:
            registry_url: Base URL of registry service (e.g., http://tossit.cc)
            cluster_id: This cluster's unique ID
            node_name: This node's name
            port: Port this node's web interface runs on
        """
        self.registry_url = registry_url.rstrip('/')
        self.cluster_id = cluster_id
        self.node_name = node_name
        self.port = port
        self.heartbeat_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # Stats to send to registry
        self.cluster_name: Optional[str] = None
        self.node_count: Optional[int] = None
        self.total_capacity_gb: Optional[float] = None
        self.used_capacity_gb: Optional[float] = None
        
        # Local IP detection
        self.local_ip: Optional[str] = None
        
    def update_stats(
        self,
        cluster_name: Optional[str] = None,
        node_count: Optional[int] = None,
        total_capacity_gb: Optional[float] = None,
        used_capacity_gb: Optional[float] = None,
        local_ip: Optional[str] = None
    ):
        """
        Update cluster statistics to send to registry
        Call this whenever cluster stats change
        """
        if cluster_name is not None:
            self.cluster_name = cluster_name
        if node_count is not None:
            self.node_count = node_count
        if total_capacity_gb is not None:
            self.total_capacity_gb = total_capacity_gb
        if used_capacity_gb is not None:
            self.used_capacity_gb = used_capacity_gb
        if local_ip is not None:
            self.local_ip = local_ip
    
    async def send_heartbeat(self) -> bool:
        """
        Send single heartbeat to registry
        
        Returns:
            True if successful, False otherwise
        """
        if not self.local_ip:
            logger.warning("Cannot send heartbeat: local IP not set")
            return False
        
        payload = {
            'cluster_id': self.cluster_id,
            'leader_ip': self.local_ip,
            'port': self.port,
            'node_name': self.node_name,
            'cluster_name': self.cluster_name,
            'node_count': self.node_count,
            'total_capacity_gb': self.total_capacity_gb,
            'used_capacity_gb': self.used_capacity_gb
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.registry_url}/api/registry/heartbeat",
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.debug(f"Heartbeat sent successfully: {data.get('message')}")
                        return True
                    else:
                        logger.warning(f"Heartbeat failed: HTTP {response.status}")
                        return False
                        
        except asyncio.TimeoutError:
            logger.warning(f"Heartbeat timeout: registry at {self.registry_url} not responding")
            return False
        except aiohttp.ClientError as e:
            logger.warning(f"Heartbeat failed: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending heartbeat: {e}")
            return False
    
    async def _heartbeat_loop(self):
        """
        Internal heartbeat loop
        Sends heartbeat every 10 seconds
        """
        logger.info(f"Started registry heartbeat loop (registry: {self.registry_url})")
        
        consecutive_failures = 0
        
        while self.is_running:
            success = await self.send_heartbeat()
            
            if success:
                consecutive_failures = 0
            else:
                consecutive_failures += 1
                
                # Log warning after 3 consecutive failures
                if consecutive_failures == 3:
                    logger.warning(
                        f"Registry heartbeat failed {consecutive_failures} times. "
                        f"Check registry availability at {self.registry_url}"
                    )
                # Log error after 10 consecutive failures
                elif consecutive_failures >= 10:
                    logger.error(
                        f"Registry heartbeat failed {consecutive_failures} times. "
                        f"Cluster may not be accessible via registry."
                    )
            
            # Wait 10 seconds before next heartbeat
            await asyncio.sleep(10)
    
    def start_heartbeat(self):
        """
        Start sending periodic heartbeats to registry
        Call this when this node becomes the leader
        """
        if self.is_running:
            logger.debug("Heartbeat already running")
            return
        
        self.is_running = True
        self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(f"Registry heartbeat started for cluster {self.cluster_id}")
    
    def stop_heartbeat(self):
        """
        Stop sending heartbeats to registry
        Call this when this node loses leadership
        """
        if not self.is_running:
            return
        
        self.is_running = False
        
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None
        
        logger.info(f"Registry heartbeat stopped for cluster {self.cluster_id}")
    
    async def test_connection(self) -> bool:
        """
        Test connection to registry service
        Returns True if registry is reachable, False otherwise
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{self.registry_url}/health",
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(
                            f"Registry connection OK: {data.get('service')} - "
                            f"{data.get('clusters_registered', 0)} clusters registered"
                        )
                        return True
                    else:
                        logger.warning(f"Registry health check failed: HTTP {response.status}")
                        return False
                        
        except Exception as e:
            logger.warning(f"Cannot reach registry at {self.registry_url}: {e}")
            return False


# Example usage in tossit_node.py:
"""
# Initialize registry client
registry_client = None
if os.getenv('TOSSIT_REGISTRY_URL'):
    registry_url = os.getenv('TOSSIT_REGISTRY_URL')
    registry_client = RegistryClient(
        registry_url=registry_url,
        cluster_id=config.cluster_id,
        node_name=config.node_name,
        port=config.port
    )
    
    # Test connection on startup
    if await registry_client.test_connection():
        logger.info(f"Registry service configured: {registry_url}")
    else:
        logger.warning(f"Registry service unreachable: {registry_url}")

# When becoming leader (in Raft callback)
def on_leadership_gained():
    if registry_client:
        # Update with current stats
        registry_client.update_stats(
            cluster_name=f"Cluster {config.node_name}",
            node_count=len(peer_nodes) + 1,
            total_capacity_gb=total_capacity,
            used_capacity_gb=used_capacity,
            local_ip=get_local_ip()
        )
        # Start heartbeat
        registry_client.start_heartbeat()

# When losing leadership (in Raft callback)
def on_leadership_lost():
    if registry_client:
        registry_client.stop_heartbeat()

# Update stats periodically
async def update_registry_stats():
    while True:
        if registry_client and is_leader:
            registry_client.update_stats(
                node_count=len(peer_nodes) + 1,
                total_capacity_gb=calculate_total_capacity(),
                used_capacity_gb=calculate_used_capacity()
            )
        await asyncio.sleep(30)  # Update every 30 seconds
"""