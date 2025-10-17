"""
TossIt Node Agent v2 - With actual chunk storage and config support
Runs on each machine in the cluster to handle storage operations and communicate with the Brain
"""

import asyncio
import aiohttp
import hashlib
from pathlib import Path
from typing import Optional
import psutil
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse
import uvicorn
import json

class NodeAgent:
    def __init__(self, 
                 node_name: str,
                 storage_path: str,
                 brain_url: str,
                 port: int = 8080,
                 config: Optional[dict] = None):
        self.node_name = node_name
        self.storage_path = Path(storage_path)
        self.brain_url = brain_url
        self.port = port
        self.config = config or {}
        
        # Ensure storage directory exists
        self.storage_path.mkdir(parents=True, exist_ok=True)
        
        # Calculate storage limits
        self._calculate_storage_limits()
        
        # Node info
        self.node_id = None
        self.is_registered = False
        
        # FastAPI app for receiving chunk requests
        self.app = FastAPI()
        self.setup_routes()
    
    def _calculate_storage_limits(self):
        """Calculate how much storage this node will dedicate to TossIt"""
        disk = psutil.disk_usage(str(self.storage_path))
        total_available_gb = disk.free / (1024**3)
        
        storage_mode = self.config.get("storage_mode", "percentage")
        
        if storage_mode == "full":
            self.storage_limit_gb = total_available_gb
            print(f"Storage mode: FULL - Using all {total_available_gb:.1f} GB available")
        elif storage_mode == "percentage":
            percent = self.config.get("storage_limit_percent", 50)
            self.storage_limit_gb = total_available_gb * (percent / 100.0)
            print(f"Storage mode: PERCENTAGE - Using {percent}% ({self.storage_limit_gb:.1f} GB of {total_available_gb:.1f} GB)")
        elif storage_mode == "fixed_gb":
            fixed = self.config.get("storage_limit_gb", 100)
            self.storage_limit_gb = min(fixed, total_available_gb)
            print(f"Storage mode: FIXED - Using {self.storage_limit_gb:.1f} GB (max: {total_available_gb:.1f} GB)")
        else:
            # Default to 50%
            self.storage_limit_gb = total_available_gb * 0.5
            print(f"Storage mode: DEFAULT (50%) - Using {self.storage_limit_gb:.1f} GB")
        
        # Add safety margin - never use more than 95% of available space
        max_safe_limit = total_available_gb * 0.95
        if self.storage_limit_gb > max_safe_limit:
            print(f"WARNING: Reducing storage limit from {self.storage_limit_gb:.1f} GB to {max_safe_limit:.1f} GB for safety")
            self.storage_limit_gb = max_safe_limit

    def _get_tossit_usage(self):
        """Calculate how much space TossIt is actually using"""
        total_size = 0
        chunk_count = 0
        chunk_files = []
        
        try:
            for chunk_file in self.storage_path.glob("chunk_*.dat"):
                file_size = chunk_file.stat().st_size
                total_size += file_size
                chunk_count += 1
                chunk_files.append({
                    'name': chunk_file.name,
                    'size_mb': file_size / (1024**2)
                })
        except Exception as e:
            print(f"Error calculating storage usage: {e}")
            return 0.0
        
        usage_gb = total_size / (1024**3)
        
        # Detailed logging every 10th heartbeat to avoid spam
        if hasattr(self, '_heartbeat_count'):
            self._heartbeat_count += 1
        else:
            self._heartbeat_count = 0
        
        if self._heartbeat_count % 10 == 0 and chunk_count > 0:
            print(f"Storage usage details:")
            print(f"  Total chunks: {chunk_count}")
            print(f"  Total usage: {usage_gb:.3f} GB")
            print(f"  Storage limit: {self.storage_limit_gb:.1f} GB")
            print(f"  Remaining: {self.storage_limit_gb - usage_gb:.3f} GB")
            print(f"  Usage percentage: {(usage_gb / self.storage_limit_gb * 100):.1f}%")
            
            # Show largest chunks
            if chunk_files:
                largest = sorted(chunk_files, key=lambda x: x['size_mb'], reverse=True)[:3]
                print(f"""  Largest chunks: {", ".join(f"{c['name']} ({c['size_mb']:.1f}MB)" for c in largest)}""")
        
        return usage_gb

    def check_storage_before_accepting(self, chunk_size_bytes):
        """Check if we can accept a new chunk without exceeding limits"""
        current_usage = self._get_tossit_usage()
        chunk_size_gb = chunk_size_bytes / (1024**3)
        
        would_exceed = (current_usage + chunk_size_gb) > self.storage_limit_gb
        
        if would_exceed:
            print(f"STORAGE LIMIT CHECK FAILED:")
            print(f"  Current usage: {current_usage:.3f} GB")
            print(f"  Chunk size: {chunk_size_gb:.3f} GB")
            print(f"  Would be: {current_usage + chunk_size_gb:.3f} GB")
            print(f"  Limit: {self.storage_limit_gb:.1f} GB")
            return False
        
        return True
        
    def setup_routes(self):
        """Setup API routes for the node"""
        
        @self.app.post("/api/chunks/store")
        async def store_chunk(chunk_id: int = Form(...), 
                            is_primary: bool = Form(False),
                            chunk_data: UploadFile = File(...)):
            """Store a chunk on this node"""
            try:
                # Read chunk data first to get size
                content = await chunk_data.read()
                chunk_size = len(content)
                
                # Check storage limits BEFORE storing
                if not self.check_storage_before_accepting(chunk_size):
                    print(f"REJECTED chunk {chunk_id}: Would exceed storage limit")
                    return {
                        "status": "rejected", 
                        "reason": "insufficient_storage",
                        "message": f"Storing chunk would exceed configured storage limit"
                    }
                
                # Save chunk to disk
                local_path = self.storage_path / f"chunk_{chunk_id}.dat"
                
                with open(local_path, 'wb') as f:
                    f.write(content)
                
                print(f"✓ Stored chunk {chunk_id} ({'primary' if is_primary else 'replica'}) - {chunk_size / (1024**2):.2f} MB")
                
                return {
                    "status": "stored",
                    "local_path": str(local_path),
                    "size": chunk_size
                }
            except Exception as e:
                print(f"✗ Failed to store chunk {chunk_id}: {e}")
                return {"status": "error", "message": str(e)}
        
        @self.app.get("/api/chunks/{chunk_id}/data")
        async def get_chunk_data(chunk_id: int):
            """Retrieve chunk data from this node"""
            local_path = self.storage_path / f"chunk_{chunk_id}.dat"
            
            if not local_path.exists():
                return {"error": "Chunk not found"}
            
            return FileResponse(local_path)
        
        @self.app.delete("/api/chunks/{chunk_id}/delete")
        async def delete_chunk(chunk_id: int):
            """Delete a chunk from this node"""
            local_path = self.storage_path / f"chunk_{chunk_id}.dat"
            
            try:
                if local_path.exists():
                    local_path.unlink()
                    print(f"✓ Deleted chunk {chunk_id}")
                    return {"status": "deleted", "chunk_id": chunk_id}
                else:
                    return {"status": "not_found", "chunk_id": chunk_id}
            except Exception as e:
                print(f"✗ Error deleting chunk {chunk_id}: {e}")
                return {"status": "error", "message": str(e)}
        
        @self.app.get("/health")
        async def health_check():
            """Health check endpoint"""
            return {"status": "ok", "node": self.node_name}
    
    async def start(self):
        """Start the node agent"""
        print(f"Starting TossIt node: {self.node_name}")
        
        # Register with Brain
        await self.register_with_brain()
        
        # Start background tasks
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.job_processor_loop())
        
        # Start FastAPI server
        config = uvicorn.Config(
            self.app, 
            host="0.0.0.0", 
            port=self.port,
            log_level="info"
        )
        server = uvicorn.Server(config)
        
        print(f"Node {self.node_name} is running on port {self.port}")
        await server.serve()
    
    async def register_with_brain(self):
        """Register this node with the Brain"""
        # Collect system metrics
        disk = psutil.disk_usage(str(self.storage_path))
        cpu_count = psutil.cpu_count()
        
        # Get actual network IP address
        import socket
        try:
            # Connect to an external address to determine our IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip_address = s.getsockname()[0]
            s.close()
        except Exception:
            # Fallback to localhost if we can't determine IP
            ip_address = "127.0.0.1"
            print("⚠ Warning: Could not determine network IP, using 127.0.0.1")
        
        node_info = {
            "name": self.node_name,
            "ip_address": ip_address,
            "port": self.port,
            "total_capacity_gb": self.storage_limit_gb,  # Use configured limit, not full disk
            "free_capacity_gb": self.storage_limit_gb,   # Initially all free
            "cpu_score": cpu_count * 1.0,
            "network_speed_mbps": 1000.0,
            "avg_uptime_percent": 99.0
        }
        
        print(f"Registering with IP: {ip_address}:{self.port}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.brain_url}/api/nodes/register",
                    json=node_info
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        self.node_id = data["node_id"]
                        self.is_registered = True
                        print(f"✓ Registered with Brain (node_id: {self.node_id})")
                    else:
                        print(f"✗ Failed to register: {response.status}")
        except Exception as e:
            print(f"✗ Could not connect to Brain: {e}")
    
    # node_agent.py - Fixed heartbeat_loop
    async def heartbeat_loop(self):
        """Send periodic heartbeats to the Brain"""
        while True:
            await asyncio.sleep(30)
            
            # FIXED: Check both is_registered AND node_id
            if not self.is_registered or self.node_id is None:
                print(f"Skipping heartbeat - not yet registered (node_id: {self.node_id})")
                continue
            
            # Calculate actual TossIt usage
            used_gb = self._get_tossit_usage()
            free_gb = max(0, self.storage_limit_gb - used_gb)  # FIXED: Ensure non-negative
            
            heartbeat_data = {
                "node_id": int(self.node_id),  # FIXED: Explicit int conversion
                "free_capacity_gb": float(free_gb),  # FIXED: Explicit float conversion
                "status": "online"
            }
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        f"{self.brain_url}/api/nodes/heartbeat",
                        json=heartbeat_data,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as response:
                        if response.status == 200:
                            print(f"Heartbeat OK (using {used_gb:.2f}/{self.storage_limit_gb:.1f} GB)")
                        else:
                            error_text = await response.text()
                            print(f"Heartbeat failed: HTTP {response.status}: {error_text}")
                            
            except asyncio.TimeoutError:
                print(f"Heartbeat timeout - brain server may be overloaded")
            except aiohttp.ClientError as e:
                print(f"Heartbeat network error: {e}")
            except Exception as e:
                print(f"Heartbeat unexpected error: {e}")
            
    async def job_processor_loop(self):
        """Check for and process pending jobs"""
        while True:
            await asyncio.sleep(5)  # Check every 5 seconds
            
            if not self.is_registered:
                continue
            
            try:
                async with aiohttp.ClientSession() as session:
                    # Ask Brain for pending jobs
                    async with session.get(
                        f"{self.brain_url}/api/jobs/next",
                        params={"node_id": self.node_id}
                    ) as response:
                        if response.status == 200:
                            job = await response.json()
                            if job:
                                await self.process_job(job)
            except Exception as e:
                print(f"Job check failed: {e}")
    
    async def process_job(self, job: dict):
        """Process a replication, verification, or rebalance job"""
        job_type = job["job_type"]
        job_id = job["id"]
        
        print(f"Processing job {job_id}: {job_type}")
        
        try:
            if job_type == "replication":
                await self.replicate_chunk(job)
            elif job_type == "verification":
                await self.verify_chunk(job)
            elif job_type == "rebalance":
                await self.rebalance_chunk(job)
            
            # Mark job complete
            await self.update_job_status(job_id, "completed")
        except Exception as e:
            print(f"Job {job_id} failed: {e}")
            await self.update_job_status(job_id, "failed", str(e))
    
    async def replicate_chunk(self, job: dict):
        """Download a chunk from source node and store locally"""
        chunk_id = job["chunk_id"]
        source_node_url = job["source_node_url"]
        
        # Download chunk from source
        local_path = self.storage_path / f"chunk_{chunk_id}.dat"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(
                f"{source_node_url}/api/chunks/{chunk_id}/data"
            ) as response:
                if response.status == 200:
                    with open(local_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
        
        # Verify checksum
        checksum = await self.calculate_checksum(local_path)
        
        # Report back to Brain
        await self.report_replica_created(chunk_id, str(local_path), checksum)
        
        print(f"✓ Replicated chunk {chunk_id}")
    
    async def rebalance_chunk(self, job: dict):
        """Move a chunk from source node to this node and update primary location"""
        chunk_id = job["chunk_id"]
        source_node_url = job["source_node_url"]
        
        print(f"🔄 Rebalancing chunk {chunk_id} from {source_node_url}")
        
        # Download chunk from source
        local_path = self.storage_path / f"chunk_{chunk_id}.dat"
        
        try:
            async with aiohttp.ClientSession() as session:
                # Download chunk data
                async with session.get(
                    f"{source_node_url}/api/chunks/{chunk_id}/data",
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    if response.status == 200:
                        with open(local_path, 'wb') as f:
                            async for chunk in response.content.iter_chunked(8192):
                                f.write(chunk)
                    else:
                        raise Exception(f"Failed to download chunk: HTTP {response.status}")
            
            # Verify checksum if provided
            expected_checksum = job.get("expected_checksum")
            if expected_checksum:
                actual_checksum = await self.calculate_checksum(local_path)
                if actual_checksum != expected_checksum:
                    local_path.unlink()  # Delete corrupted file
                    raise Exception(f"Checksum mismatch! Expected {expected_checksum}, got {actual_checksum}")
            
            # Report successful migration to Brain
            await self.report_rebalance_complete(chunk_id, str(local_path))
            
            print(f"✓ Successfully rebalanced chunk {chunk_id}")
            
        except Exception as e:
            # Clean up partial file
            if local_path.exists():
                local_path.unlink()
            raise e
    
    async def verify_chunk(self, job: dict):
        """Verify a chunk's integrity"""
        chunk_id = job["chunk_id"]
        expected_checksum = job["expected_checksum"]
        local_path = Path(job["local_path"])
        
        if not local_path.exists():
            raise Exception(f"Chunk file not found: {local_path}")
        
        actual_checksum = await self.calculate_checksum(local_path)
        
        if actual_checksum != expected_checksum:
            raise Exception(f"Checksum mismatch! Expected {expected_checksum}, got {actual_checksum}")
        
        print(f"✓ Verified chunk {chunk_id}")
    
    async def calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file"""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    async def update_job_status(self, job_id: int, status: str, error: Optional[str] = None):
        """Update job status on the Brain"""
        update_data = {
            "job_id": job_id,
            "status": status,
            "error_message": error
        }
        
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{self.brain_url}/api/jobs/update",
                json=update_data
            )
    
    async def report_replica_created(self, chunk_id: int, local_path: str, checksum: str):
        """Report to Brain that a replica has been created"""
        replica_data = {
            "chunk_id": chunk_id,
            "node_id": self.node_id,
            "local_path": local_path,
            "checksum": checksum
        }
        
        async with aiohttp.ClientSession() as session:
            await session.post(
                f"{self.brain_url}/api/replicas/create",
                json=replica_data
            )
    
    async def report_rebalance_complete(self, chunk_id: int, local_path: str):
        """Report to Brain that rebalancing is complete"""
        migration_data = {
            "chunk_id": chunk_id,
            "new_node_id": self.node_id,
            "local_path": local_path
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.brain_url}/api/rebalance/complete",
                    json=migration_data
                ) as response:
                    if response.status == 200:
                        print(f"✓ Reported rebalance completion for chunk {chunk_id}")
                    else:
                        print(f"⚠ Failed to report rebalance completion: HTTP {response.status}")
        except Exception as e:
            print(f"⚠ Error reporting rebalance completion: {e}")


async def main():
    import sys
    
    # Check for config file flag
    config = {}
    config_file = None
    
    if "--config" in sys.argv:
        config_idx = sys.argv.index("--config")
        if config_idx + 1 < len(sys.argv):
            config_file = sys.argv[config_idx + 1]
            try:
                with open(config_file, 'r') as f:
                    loaded_config = json.load(f)
                    config = loaded_config.get("node", {})
                print(f"✓ Loaded configuration from {config_file}")
            except Exception as e:
                print(f"✗ Error loading config file: {e}")
                print("Using command-line arguments instead")
    
    # If config file was used, get values from there
    if config:
        node_name = config.get("name", "tossit-node")
        storage_path = config.get("storage_path", "./storage")
        brain_url = config.get("brain_url", "http://localhost:8000")
        port = config.get("port", 8080)
    else:
        # Use command line arguments
        if len(sys.argv) < 4:
            print("Usage: python3 node_agent.py <node_name> <storage_path> <brain_url> [--port PORT] [--config CONFIG_FILE]")
            print("Example: python3 node_agent.py kevin-nas /data/tossit http://192.168.74.130:8000 --port 8081")
            print("Or with config: python3 node_agent.py --config tossit_config.json")
            sys.exit(1)
        
        node_name = sys.argv[1]
        storage_path = sys.argv[2]
        brain_url = sys.argv[3]
        
        # Parse optional port argument
        port = 8080
        if "--port" in sys.argv:
            port_idx = sys.argv.index("--port")
            if port_idx + 1 < len(sys.argv):
                port = int(sys.argv[port_idx + 1])
    
    agent = NodeAgent(node_name, storage_path, brain_url, port, config)
    await agent.start()


if __name__ == "__main__":
    asyncio.run(main())