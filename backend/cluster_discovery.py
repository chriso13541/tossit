#!/usr/bin/env python3
"""
TossIt Cluster Discovery - mDNS Implementation

Nodes broadcast their presence and discover other nodes in the same cluster
using mDNS (multicast DNS) for local network discovery.
"""

import asyncio
import socket
import json
from datetime import datetime, timezone
from typing import Dict, Set, Callable, Optional
from zeroconf import ServiceInfo, Zeroconf
from zeroconf.asyncio import AsyncZeroconf, AsyncServiceBrowser


class ClusterDiscovery:
    """
    Handles node discovery within a cluster using mDNS
    
    Each node broadcasts:
    - Cluster ID
    - Node ID  
    - Node name
    - IP address
    - Port
    - Storage capacity
    """
    
    def __init__(
        self,
        cluster_id: str,
        node_id: str,
        node_name: str,
        port: int,
        storage_gb: float,
        on_node_discovered: Optional[Callable] = None,
        on_node_lost: Optional[Callable] = None
    ):
        self.cluster_id = cluster_id
        self.node_id = node_id
        self.node_name = node_name
        self.port = port
        self.storage_gb = storage_gb
        
        # Callbacks
        self.on_node_discovered = on_node_discovered
        self.on_node_lost = on_node_lost
        
        # State
        self.discovered_nodes: Dict[str, dict] = {}  # node_id -> node_info
        self.aiozc: Optional[AsyncZeroconf] = None
        self.browser: Optional[AsyncServiceBrowser] = None
        self.service_info: Optional[ServiceInfo] = None
        
        # Service type for TossIt nodes
        self.service_type = "_tossit._tcp.local."
        
    async def start(self):
        """Start broadcasting and listening for other nodes"""
        print(f"Starting cluster discovery for cluster: {self.cluster_id}")
        
        # Initialize zeroconf
        self.aiozc = AsyncZeroconf()
        
        # Register our service
        await self._register_service()
        
        # Start browsing for other nodes
        await self._start_browser()
        
        print(f"Discovery active - broadcasting as {self.node_name}")
    
    async def stop(self):
        """Stop discovery and unregister service"""
        print("Stopping cluster discovery...")
        
        if self.browser:
            await self.browser.async_cancel()
        
        if self.service_info and self.aiozc:
            await self.aiozc.async_unregister_service(self.service_info)
        
        if self.aiozc:
            await self.aiozc.async_close()
        
        print("✓ Discovery stopped")
    
    async def _register_service(self):
        """Register this node as a mDNS service"""
        # Get local IP
        hostname = socket.gethostname()
        local_ip = self._get_local_ip()
        
        # Create service name: cluster_id-node_id
        service_name = f"{self.cluster_id}-{self.node_id}.{self.service_type}"
        
        # Service properties (metadata)
        properties = {
            'cluster_id': self.cluster_id,
            'node_id': self.node_id,
            'node_name': self.node_name,
            'storage_gb': str(self.storage_gb),
            'version': '2.0'
        }
        
        # Create service info
        self.service_info = ServiceInfo(
            type_=self.service_type,
            name=service_name,
            addresses=[socket.inet_aton(local_ip)],
            port=self.port,
            properties=properties,
            server=f"{hostname}.local."
        )
        
        # Register with zeroconf
        await self.aiozc.async_register_service(self.service_info)
        
        print(f"✓ Broadcasting on {local_ip}:{self.port}")
    
    async def _start_browser(self):
        """Start browsing for TossIt services"""
        from zeroconf import ServiceStateChange
        
        def on_service_state_change(
            zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange
        ):
            """Callback for service changes - schedules async handlers"""
            if state_change is ServiceStateChange.Added:
                # Service added - get info asynchronously
                asyncio.create_task(self._async_service_added(service_type, name))
            
            elif state_change is ServiceStateChange.Removed:
                # Service removed
                asyncio.create_task(self._handle_service_removed(name))
            
            elif state_change is ServiceStateChange.Updated:
                # Service updated
                asyncio.create_task(self._async_service_added(service_type, name))
        
        # Create browser with callback function
        self.browser = AsyncServiceBrowser(
            self.aiozc.zeroconf,
            self.service_type,
            handlers=[on_service_state_change]
        )
    
    async def _async_service_added(self, service_type: str, name: str):
        """Async handler for service added - properly gets service info"""
        try:
            # Use async method to get service info
            info = await self.aiozc.async_get_service_info(service_type, name)
            if info:
                await self._handle_service_added(info)
        except Exception as e:
            print(f"Error getting service info: {e}")
    
    async def _handle_service_added(self, info: ServiceInfo):
        """Handle discovered service"""
        try:
            # Extract properties
            props = {
                k.decode() if isinstance(k, bytes) else k: 
                v.decode() if isinstance(v, bytes) else v
                for k, v in info.properties.items()
            }
            
            discovered_cluster_id = props.get('cluster_id', '')
            discovered_node_id = props.get('node_id', '')
            
            # Only care about nodes in our cluster
            if discovered_cluster_id != self.cluster_id:
                return
            
            # Don't discover ourselves
            if discovered_node_id == self.node_id:
                return
            
            # Get IP address
            if info.addresses:
                ip_address = socket.inet_ntoa(info.addresses[0])
            else:
                return
            
            # Build node info
            node_info = {
                'node_id': discovered_node_id,
                'node_name': props.get('node_name', 'Unknown'),
                'ip_address': ip_address,
                'port': info.port,
                'storage_gb': float(props.get('storage_gb', 0)),
                'cluster_id': discovered_cluster_id,
                'discovered_at': datetime.now(timezone.utc).isoformat(),
                'service_name': info.name
            }
            
            # Check if new or updated
            if discovered_node_id not in self.discovered_nodes:
                print(f"✨ Discovered node: {node_info['node_name']} at {ip_address}:{info.port}")
                self.discovered_nodes[discovered_node_id] = node_info
                
                # Callback
                if self.on_node_discovered:
                    await self.on_node_discovered(node_info)
            else:
                # Update existing
                self.discovered_nodes[discovered_node_id].update(node_info)
        
        except Exception as e:
            print(f"Error handling discovered service: {e}")
    
    async def _handle_service_removed(self, name: str):
        """Handle service that disappeared"""
        try:
            # Find which node this was
            for node_id, info in list(self.discovered_nodes.items()):
                if info.get('service_name') == name:
                    print(f"Node left: {info['node_name']}")
                    del self.discovered_nodes[node_id]
                    
                    # Callback
                    if self.on_node_lost:
                        await self.on_node_lost(node_id)
                    break
        
        except Exception as e:
            print(f"Error handling removed service: {e}")
    
    def get_discovered_nodes(self) -> Dict[str, dict]:
        """Get all currently discovered nodes"""
        return self.discovered_nodes.copy()
    
    def _get_local_ip(self) -> str:
        """Get local IP address"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return "127.0.0.1"


# Example usage
async def main():
    """Test discovery"""
    
    async def node_discovered(node_info):
        print(f"Callback: Node discovered - {node_info}")
    
    async def node_lost(node_id):
        print(f"Callback: Node lost - {node_id}")
    
    # Create discovery instance
    discovery = ClusterDiscovery(
        cluster_id="test1234",
        node_id="node001",
        node_name="test-node",
        port=8000,
        storage_gb=100.0,
        on_node_discovered=node_discovered,
        on_node_lost=node_lost
    )
    
    # Start discovery
    await discovery.start()
    
    # Run for a while
    try:
        while True:
            await asyncio.sleep(5)
            nodes = discovery.get_discovered_nodes()
            print(f"Currently discovered nodes: {len(nodes)}")
            for node_id, info in nodes.items():
                print(f"  - {info['node_name']} ({info['ip_address']}:{info['port']})")
    except KeyboardInterrupt:
        pass
    finally:
        await discovery.stop()


if __name__ == "__main__":
    asyncio.run(main())
