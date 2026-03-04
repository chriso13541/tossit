#!/usr/bin/env python3
"""
TossIt v2.0 - Unified Node (Brain + Storage)
Integrated with mDNS Discovery, Health Monitoring, and Chunk Replication
"""

import asyncio
import hashlib
import secrets
import socket
import sys
import os
import shutil
import time
import threading
from pathlib import Path
from typing import Optional, Dict
import yaml
import aiohttp
from datetime import datetime, timezone

# New Discovery and Raft Imports
from cluster_discovery import ClusterDiscovery
from cluster_raft import ClusterRaft
from node_health_monitor import NodeHealthMonitor
from registry_client import RegistryClient

# SECURITY: PKI Imports
from node_identity import NodeIdentity
from trust_store import TrustStore

import tempfile
from fastapi import BackgroundTasks


from fastapi import FastAPI, UploadFile, File as FastAPIFile, HTTPException, Query
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
import uvicorn


# Maximum upload size (10 GB)
MAX_UPLOAD_SIZE_BYTES = 10 * 1024 * 1024 * 1024


async def db_commit_with_retry(db, max_retries=5, base_delay=0.05):
    """
    Commit a database transaction with retry on 'database is locked'.
    
    SQLite only allows one writer at a time. Under concurrent uploads,
    commits can collide. This retries with exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            db.commit()
            return
        except OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"DB locked on commit (attempt {attempt + 1}/{max_retries}), retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)
                continue
            raise


async def db_flush_with_retry(db, max_retries=5, base_delay=0.05):
    """
    Flush (send SQL without committing) with retry on 'database is locked'.
    
    flush() acquires a write lock on SQLite. Even with busy_timeout, pooled
    connections can sometimes race. This retries with exponential backoff.
    """
    for attempt in range(max_retries):
        try:
            db.flush()
            return
        except OperationalError as e:
            if "locked" in str(e).lower() and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                print(f"DB locked on flush (attempt {attempt + 1}/{max_retries}), retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)
                continue
            raise

from models import (
    Base, Node, File as FileModel, Chunk, FileChunk, ChunkLocation, Job, AuditLog,
    NodeStatus, JobStatus, JobType
)


# Configuration paths
CONFIG_DIR = Path.home() / ".tossit"
CONFIG_FILE = CONFIG_DIR / "node_config.yaml"
STORAGE_DIR = CONFIG_DIR / "storage"
DB_DIR = CONFIG_DIR / "database"

# Ensure directories exist
CONFIG_DIR.mkdir(exist_ok=True)
STORAGE_DIR.mkdir(exist_ok=True)
DB_DIR.mkdir(exist_ok=True)


def get_disk_usage(path: Path) -> tuple[float, float, float]:
    """Get disk usage for path in GB (for setup only)"""
    stat = shutil.disk_usage(path)
    total_gb = stat.total / (1024 ** 3)
    used_gb = stat.used / (1024 ** 3)
    free_gb = stat.free / (1024 ** 3)
    return total_gb, used_gb, free_gb


def get_storage_usage(storage_path: Path) -> float:
    """
    Calculate actual storage used by TossIt chunk data.
    Scans both content-addressed chunks/ dir and legacy file_*.dat files.
    Returns: used_gb
    """
    total_bytes = 0
    
    if storage_path.exists():
        # Content-addressed chunks (new layout)
        chunks_dir = storage_path / "chunks"
        if chunks_dir.exists():
            for chunk_file in chunks_dir.glob("*.dat"):
                try:
                    total_bytes += chunk_file.stat().st_size
                except Exception:
                    pass
        
        # Legacy file_*.dat (old layout, for migration)
        for file_path in storage_path.glob("file_*.dat"):
            try:
                total_bytes += file_path.stat().st_size
            except Exception:
                pass
    
    used_gb = total_bytes / (1024 ** 3)
    return used_gb




class NodeConfig:
    """Node configuration with persistent storage"""
    
    def __init__(self):
        self.node_id: Optional[str] = None
        self.node_name: Optional[str] = None
        self.cluster_id: Optional[str] = None
        self.cluster_mode: str = "private"
        self.is_first_node: bool = False
        self.port: int = 8000
        self.storage_path: Path = STORAGE_DIR
        self.storage_limit_gb: float = 50.0  # Default 50GB
        self.center_enabled: bool = False
        self.center_id: Optional[str] = None
        
    @staticmethod
    def generate_cluster_id() -> str:
        """Generate 8-character cluster ID"""
        return secrets.token_hex(4)
    
    @staticmethod
    def generate_node_id() -> str:
        """Generate unique node ID"""
        return secrets.token_hex(8)
    
    def save(self):
        """Save configuration to disk"""
        config_data = {
            'node_id': self.node_id,
            'node_name': self.node_name,
            'cluster_id': self.cluster_id,
            'cluster_mode': self.cluster_mode,
            'is_first_node': self.is_first_node,
            'port': self.port,
            'storage_path': str(self.storage_path),
            'storage_limit_gb': self.storage_limit_gb,
            'center_enabled': self.center_enabled,
            'center_id': self.center_id,
        }
        
        with open(CONFIG_FILE, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False)
        
        print(f"✓ Configuration saved to {CONFIG_FILE}")
    
    @staticmethod
    def load() -> 'NodeConfig':
        """Load configuration from disk"""
        config = NodeConfig()
        
        if not CONFIG_FILE.exists():
            return config
        
        try:
            with open(CONFIG_FILE, 'r') as f:
                data = yaml.safe_load(f)
            
            if data:
                config.node_id = data.get('node_id')
                config.node_name = data.get('node_name')
                config.cluster_id = data.get('cluster_id')
                config.cluster_mode = data.get('cluster_mode', 'private')
                config.is_first_node = data.get('is_first_node', False)
                config.port = data.get('port', 8000)
                config.storage_path = Path(data.get('storage_path', STORAGE_DIR))
                config.storage_limit_gb = data.get('storage_limit_gb', 50.0)
                config.center_enabled = data.get('center_enabled', False)
                config.center_id = data.get('center_id')
            
            print(f"✓ Configuration loaded from {CONFIG_FILE}")
            return config
            
        except Exception as e:
            print(f"Error loading config: {e}")
            return config
    
    def exists(self) -> bool:
        """Check if configuration exists"""
        return CONFIG_FILE.exists() and self.node_id is not None


def get_local_ip() -> str:
    """Get local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception:
        return "127.0.0.1"


def interactive_setup() -> NodeConfig:
    """Interactive first-time setup"""
    print("\n" + "="*60)
    print("TossIt v2.0 - First Time Setup")
    print("="*60)
    print()
    
    config = NodeConfig()
    
    # Node name
    default_name = socket.gethostname()
    node_name = input(f"Node name [{default_name}]: ").strip()
    config.node_name = node_name if node_name else default_name
    
    # Generate node ID
    config.node_id = NodeConfig.generate_node_id()
    print(f"✓ Generated node ID: {config.node_id}")
    
    # Storage allocation
    print("\n--- Storage Configuration ---")
    total_gb, used_gb, free_gb = get_disk_usage(Path.home())
    print(f"Available disk space: {free_gb:.1f} GB free of {total_gb:.1f} GB total")
    
    while True:
        storage_input = input(f"\nStorage to allocate (GB) [50]: ").strip()
        
        if not storage_input:
            # User pressed enter, use default
            config.storage_limit_gb = min(50.0, free_gb * 0.9)
            break
        
        # Try to parse as number
        try:
            requested = float(storage_input)
            if requested <= 0:
                print("Storage must be greater than 0")
                continue
            if requested > free_gb:
                print(f"Only {free_gb:.1f} GB available")
                use_max = input(f"Use maximum available ({free_gb * 0.9:.1f} GB)? [y/n]: ").strip().lower()
                if use_max == 'y':
                    config.storage_limit_gb = free_gb * 0.9
                    break
                else:
                    continue
            else:
                config.storage_limit_gb = requested
                break
        except ValueError:
            print("Please enter a valid number")
            continue
    
    print(f"✓ Allocated: {config.storage_limit_gb:.1f} GB")
    
    # Cluster setup
    print("\n--- Cluster Setup ---")
    print("1. Create new cluster (I'm the first node)")
    print("2. Join existing cluster (someone else already created it)")
    
    choice = input("\nChoose [1/2]: ").strip()
    
    if choice == "1":
        config.is_first_node = True
        config.cluster_id = NodeConfig.generate_cluster_id()
        print(f"\n✓ Created new cluster: {config.cluster_id}")
        print(f"\n  IMPORTANT: Share this cluster ID with others to join:")
        print(f" {config.cluster_id}")
        print()
    elif choice == "2":
        config.is_first_node = False
        cluster_id = input("\nEnter cluster ID (8 characters): ").strip()
        
        if len(cluster_id) != 8:
            print("Invalid cluster ID (must be 8 characters)")
            print("Starting as single-node cluster instead...")
            config.cluster_id = NodeConfig.generate_cluster_id()
            config.is_first_node = True
        else:
            config.cluster_id = cluster_id
            print(f"✓ Will join cluster: {cluster_id}")
    else:
        print("Invalid choice, creating new cluster...")
        config.is_first_node = True
        config.cluster_id = NodeConfig.generate_cluster_id()
    
    # Port configuration
    port_input = input(f"\nPort number [8000]: ").strip()
    if port_input and port_input.isdigit():
        config.port = int(port_input)
    
    config.cluster_mode = "private"
    config.center_enabled = False
    
    print("\n--- Summary ---")
    print(f"Node Name:     {config.node_name}")
    print(f"Node ID:       {config.node_id}")
    print(f"Storage:       {config.storage_limit_gb:.1f} GB")
    print(f"Cluster ID:    {config.cluster_id}")
    print(f"Cluster Role:  {'First Node' if config.is_first_node else 'Joining Node'}")
    print(f"Port:          {config.port}")
    print()
    
    config.save()
    return config


class TossItNode:
    """Unified node that acts as both brain and storage"""
    
    def __init__(self, config: NodeConfig):
        self.config = config
        self.app = FastAPI(title=f"TossIt Node - {config.node_name}")
        
        # CRITICAL: Configure temp directory on disk (not tmpfs) for Debian compatibility
        # Must be done BEFORE any file handling to prevent RAM buffering
        import tempfile
        import os
        self.config.storage_path.mkdir(parents=True, exist_ok=True)
        upload_temp_dir = self.config.storage_path / "upload_temp"
        upload_temp_dir.mkdir(exist_ok=True, parents=True)
        os.environ['TMPDIR'] = str(upload_temp_dir)
        tempfile.tempdir = str(upload_temp_dir)
        print(f"✓ Upload temp: {upload_temp_dir} (bypasses tmpfs)")
        
        # Content-addressed storage directories
        self.chunks_dir = self.config.storage_path / "chunks"
        self.chunks_dir.mkdir(exist_ok=True, parents=True)
        self.staging_dir = self.config.storage_path / "staging"
        self.staging_dir.mkdir(exist_ok=True, parents=True)
        print(f"✓ Content-addressed storage: {self.chunks_dir}")
        print(f"✓ Upload staging: {self.staging_dir}")

        # SECURITY: Initialize cryptographic identity
        keys_path = Path.home() / ".tossit" / "keys" / config.node_name
        self.identity = NodeIdentity(config.node_name, keys_path)
        
        # SECURITY: Initialize trust store
        trust_store_path = Path.home() / ".tossit" / "trust_store.json"
        self.trust_store = TrustStore(trust_store_path)
        
        # Use cryptographic node ID (overrides the config.node_id)
        self.node_id = self.identity.get_node_id()
        
        print(f" Node identity:")
        print(f" Name:       {config.node_name}")
        print(f" ID:         {self.node_id}")
        print(f" Public key: {self.identity.get_public_key_base64()[:32]}...")

        # Discovery and Raft setup
        self.discovery: Optional[ClusterDiscovery] = None
        self.raft: Optional[ClusterRaft] = None
        self.peer_nodes: Dict[str, dict] = {}  # node_id -> node_info
        
        # Health monitoring
        self.health_monitor: Optional[NodeHealthMonitor] = None
        self.peer_refresh_task: Optional[asyncio.Task] = None
        
        # Registry client (optional)
        self.registry_client: Optional[RegistryClient] = None
        
        # Space reservation system (allows parallel uploads with atomic reservations)
        self.reservation_lock = asyncio.Lock()
        self.reserved_space_gb = 0.0  # Space reserved but not yet committed
        
        # Capacity cache (avoids DB query inside reservation lock)
        self._cached_capacity_time = 0.0
        self._capacity_cache_ttl = 2.0  # Refresh capacity every 2 seconds
        
        # Persistent aiohttp session for inter-node communication
        self._http_session: Optional[aiohttp.ClientSession] = None
        
        # DB write semaphore - serializes short DB write bursts to avoid pile-ups
        self._db_write_semaphore = asyncio.Semaphore(1)
        
        # Upload concurrency limiter - controls how many uploads can stream to disk
        # simultaneously. Prevents I/O thrashing when many clients upload at once.
        # Uploads beyond this limit queue up and start as slots free.
        self._upload_semaphore = asyncio.Semaphore(3)  # 3 concurrent streams max
        
        # UPLOAD LOAD BALANCER STATE
        # Tracks how many uploads the leader has assigned to each node (including self).
        # This is the leader's view of cluster load — used to distribute uploads evenly
        # BEFORE any node gets saturated, not reactively after I/O lockup.
        # Key: node_id (or "self"), Value: number of assigned-but-not-yet-completed uploads
        self._assigned_uploads: Dict[str, int] = {}
        self._assigned_uploads_lock = asyncio.Lock()
        
        # Replication concurrency limiter - controls how many chunks are in-flight
        # to peers simultaneously. Prevents overwhelming receiving nodes with
        # concurrent 64MB transfers that saturate I/O and cause health check timeouts.
        self._replication_semaphore = asyncio.Semaphore(4)  # 4 chunks in-flight max
        
        # Database setup
        db_path = DB_DIR / f"tossit_{config.node_id}.db"
        self.engine = create_engine(
            f'sqlite:///{db_path}',
            connect_args={
                "check_same_thread": False,
                "timeout": 30,  # Wait up to 30s for locks (SQLite busy_timeout)
            },
            echo=False,
            pool_pre_ping=True,  # Verify connections before use
        )
        
        # CRITICAL: Apply PRAGMAs on EVERY connection from the pool.
        # SQLAlchemy's connection pool creates multiple connections. Setting
        # PRAGMAs once only affects one connection — the rest get SQLite defaults
        # (busy_timeout=0, synchronous=FULL). This event fires for each connection.
        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragmas(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA wal_autocheckpoint=1000")
            cursor.execute("PRAGMA busy_timeout=30000")  # 30s in milliseconds
            cursor.close()
        
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)
        
        self.is_leader = config.is_first_node
        self._leadership_lost_at = 0  # Timestamp of last leadership loss (for grace period)
        self.local_ip = get_local_ip()
        
        # Clean up any temp files from interrupted uploads
        self._cleanup_temp_files()

        self._ensure_node_record()
        
        # Calculate actual disk usage
        self._update_capacity()
        
        self._setup_routes()
        print(f"✓ Database initialized: {db_path}")
    
    async def _on_node_discovered(self, node_info: dict):
        """Called when a new node is discovered"""
        node_id = node_info['node_id']
        self.peer_nodes[node_id] = node_info
        
        print(f"Peer joined: {node_info['node_name']} ({node_info['ip_address']})")
        
        # Update Raft with new peer list
        if self.raft:
            self.raft.update_peers(self.peer_nodes)
        
        # Start monitoring this peer's health
        if self.health_monitor:
            self.health_monitor.update_peer(node_id)
        
        # NEW: If we're the leader and this is a new node, trigger redistribution
        # This ensures existing files get replicated to the new node
        if self.raft and self.raft.is_leader():
            # Give the new node a moment to fully initialize
            await asyncio.sleep(2)
            
            print(f"Leader initiating redistribution for new node...")
            await self._redistribute_existing_files()
    
    async def _on_node_lost(self, node_id: str):
        """Called when a node leaves"""
        if node_id in self.peer_nodes:
            node_info = self.peer_nodes[node_id]
            print(f"Peer left cluster: {node_info['node_name']}")
            del self.peer_nodes[node_id]
            
            # Update Raft with new peer list
            if self.raft:
                self.raft.update_peers(self.peer_nodes)
    
    async def _on_node_went_offline(self, node_id: str):
        """Called when health monitor detects a node is offline.
        
        IMPORTANT: We do NOT remove the node from Raft peers here.
        
        The health monitor pings peers via the main asyncio event loop,
        which can be congested during heavy uploads. Before trusting the
        offline verdict, we check Raft's heartbeat thread — which runs
        independently and has its own direct communication with peers.
        If the heartbeat thread has seen the peer recently, it's alive.
        """
        # Check if the heartbeat thread (independent of event loop) has
        # communicated with this peer recently. If so, the health monitor
        # is wrong — its event-loop-based pings just couldn't get through.
        if self.raft and self.raft.is_peer_alive(node_id, max_age=30.0):
            return  # Heartbeat thread says peer is alive — ignore health monitor
        
        if node_id in self.peer_nodes:
            node_info = self.peer_nodes[node_id]
            print(f" Node went offline: {node_info['node_name']} (health monitor timeout, confirmed by heartbeat thread)")
            print(f" Raft peers unchanged — Raft handles leader detection independently")
            
            # Note: Keep in peer_nodes for now so UI can show offline status
    
    async def _periodic_peer_refresh(self):
        """Periodically check peer health, measure latency, and update capacity info"""
        print("Periodic peer health check started (every 5s)")
        
        while True:
            try:
                await asyncio.sleep(5.0)
                
                # Sync local load balancer counter with reality
                local_active = 3 - self._upload_semaphore._value
                our_self_count = self._assigned_uploads.get("self", 0)
                if our_self_count > local_active:
                    self._assigned_uploads["self"] = local_active
                
                # Health check each peer and measure latency
                if self.health_monitor and len(self.peer_nodes) > 0:
                    for node_id in list(self.peer_nodes.keys()):
                        # Try to ping the peer's health endpoint
                        peer_info = self.peer_nodes[node_id]
                        peer_url = f"http://{peer_info['ip_address']}:{peer_info['port']}/api/health"
                        
                        try:
                            # Measure round-trip time (RTT)
                            start_time = time.time()
                            
                            session = await self._get_http_session()
                            async with session.get(
                                peer_url,
                                timeout=aiohttp.ClientTimeout(total=2.0)
                            ) as response:
                                if response.status == 200:
                                    # Calculate latency in milliseconds
                                    rtt_ms = (time.time() - start_time) * 1000
                                    
                                    # Store latency in peer info
                                    peer_info['latency_ms'] = round(rtt_ms, 2)
                                    
                                    # Parse health response to get capacity info
                                    try:
                                        health_data = await response.json()
                                        
                                        # Update capacity info from peer
                                        if 'free_capacity_gb' in health_data:
                                            peer_info['free_capacity_gb'] = health_data['free_capacity_gb']
                                        if 'used_capacity_gb' in health_data:
                                            peer_info['used_capacity_gb'] = health_data['used_capacity_gb']
                                        if 'upload_slots_available' in health_data:
                                            peer_info['upload_slots_available'] = health_data['upload_slots_available']
                                            
                                            # Sync load balancer counter with reality.
                                            # If peer reports 3 slots free but our counter says 2 active,
                                            # the uploads completed and we missed the notification.
                                            peer_actual_active = 3 - health_data['upload_slots_available']
                                            our_count = self._assigned_uploads.get(node_id, 0)
                                            if our_count > peer_actual_active:
                                                self._assigned_uploads[node_id] = peer_actual_active
                                            
                                    except Exception:
                                        # Failed to parse JSON, but peer is still alive
                                        pass
                                    
                                    # Peer is alive, update timestamp
                                    self.health_monitor.update_peer(node_id)
                        except Exception:
                            # Peer didn't respond - health monitor will mark offline after timeout
                            pass
            
            except asyncio.CancelledError:
                print("Periodic peer health check stopped")
                break
            except Exception as e:
                print(f"Peer health check error: {e}")
    
    # Raft callbacks
    async def _on_became_leader(self):
        """Called when this node becomes leader"""
        self.is_leader = True
        print("This node is now the cluster LEADER")
        print(f" → Web UI available at: http://{self.local_ip}:{self.config.port}")
        print(f" → Uploads accepted here")
        
        # Start registry heartbeat if configured
        if self.registry_client:
            self.registry_client.update_stats(
                cluster_name=f"Cluster {self.config.node_name}",
                node_count=len(self.peer_nodes) + 1,
                total_capacity_gb=self._calculate_total_capacity(),
                used_capacity_gb=self._calculate_used_capacity(),
                local_ip=self.local_ip
            )
            self.registry_client.start_heartbeat()
            print(f" Started registry heartbeat for cluster {self.config.cluster_id}")
        
        # Run metadata reconciliation in background (doesn't block uploads)
        asyncio.create_task(self._reconcile_on_promotion())
    
    async def _on_lost_leadership(self):
        """Called when this node loses leadership"""
        self.is_leader = False
        self._leadership_lost_at = time.time()  # Track for grace period
        print("This node is no longer the leader")
        print("→ Uploads should go to the leader node")
        
        # Stop registry heartbeat if configured
        if self.registry_client:
            self.registry_client.stop_heartbeat()
            print("Stopped registry heartbeat")
    
    def _cleanup_temp_files(self):
        """Remove leftover staging directories and temp files from interrupted uploads"""
        import glob
        import shutil as _shutil
        
        # Clean content-addressed staging dirs
        if self.staging_dir.exists():
            staging_dirs = list(self.staging_dir.iterdir())
            if staging_dirs:
                for d in staging_dirs:
                    try:
                        _shutil.rmtree(d)
                    except Exception:
                        pass
                print(f" Cleaned up {len(staging_dirs)} staging directories")
        
        # Clean legacy temp files
        pattern = str(self.config.storage_path / "tmp_*_chunk_*.dat")
        temp_files = glob.glob(pattern)
        if temp_files:
            for f in temp_files:
                try:
                    Path(f).unlink()
                except Exception:
                    pass
            print(f" Cleaned up {len(temp_files)} legacy temp files")
    
    def _ensure_node_record(self):
        """Ensure this node exists in the database"""
        db = self.SessionLocal()
        try:
            node = db.query(Node).filter(Node.id == 1).first()
            
            if not node:
                node = Node(
                    id=1,
                    name=self.config.node_name,
                    ip_address=self.local_ip,
                    port=self.config.port,
                    total_capacity_gb=self.config.storage_limit_gb,
                    free_capacity_gb=self.config.storage_limit_gb,
                    cpu_score=1.0,
                    network_speed_mbps=100.0,
                    avg_uptime_percent=100.0,
                    priority_score=1.0,
                    status=NodeStatus.ONLINE,
                    is_brain=self.is_leader,
                    last_heartbeat=datetime.now(timezone.utc)
                )
                db.add(node)
                db.commit()
                print(f"✓ Created node record in database (ID: 1)")
            else:
                node.name = self.config.node_name
                node.ip_address = self.local_ip
                node.port = self.config.port
                node.total_capacity_gb = self.config.storage_limit_gb
                node.status = NodeStatus.ONLINE
                node.last_heartbeat = datetime.now(timezone.utc)
                db.commit()
                print(f"✓ Updated existing node record (ID: 1)")
        finally:
            db.close()
    
    def _update_capacity(self):
        """Update capacity from actual TossIt file storage"""
        # Get actual storage used by counting replicas in database
        used_gb = self._calculate_used_capacity()
        
        # Use configured limits
        self.total_capacity_gb = self.config.storage_limit_gb
        self.used_capacity_gb = min(used_gb, self.config.storage_limit_gb)
        self.free_capacity_gb = self.config.storage_limit_gb - self.used_capacity_gb
    
    def _ensure_chunk(self, db, chunk_hash: str, size_bytes: int, increment_ref: bool = True):
        """
        Ensure a Chunk row exists for the given hash. Insert-if-not-exists.
        
        This is the atomic guard against concurrent uploads of identical data.
        The Chunk table is the canonical record of chunk existence — FileChunk
        and ChunkLocation both FK to it.
        
        Args:
            db: Active SQLAlchemy session (caller handles commit)
            chunk_hash: SHA-256 hex of chunk bytes
            size_bytes: Chunk size (only used on first insert)
            increment_ref: If True and chunk already exists, bump refcount.
                           Set False when adding ChunkLocations without new FileChunks.
        
        Returns:
            The Chunk ORM object (existing or newly created)
        """
        existing = db.query(Chunk).filter(Chunk.chunk_hash == chunk_hash).first()
        if existing:
            if increment_ref:
                existing.refcount += 1
            return existing
        
        chunk = Chunk(
            chunk_hash=chunk_hash,
            size_bytes=size_bytes,
            refcount=1,
        )
        db.add(chunk)
        return chunk
    
    def _decrement_chunk_ref(self, db, chunk_hash: str) -> bool:
        """
        Decrement a chunk's refcount. If it hits 0, delete the Chunk row
        (cascades to ChunkLocations) and remove the physical file from disk.
        
        Returns:
            True if chunk was garbage collected, False if still referenced.
        """
        chunk = db.query(Chunk).filter(Chunk.chunk_hash == chunk_hash).first()
        if not chunk:
            return False
        
        chunk.refcount -= 1
        
        if chunk.refcount <= 0:
            # No more references — garbage collect
            chunk_path = self.chunks_dir / f"{chunk_hash}.dat"
            if chunk_path.exists():
                chunk_path.unlink()
            db.delete(chunk)  # Cascades to ChunkLocations
            return True
        
        return False
    
    async def _verify_chunks_on_boot(self):
        """
        Integrity verification — run once on startup.
        
        Since chunks are content-addressed, filename == SHA-256 hash of contents.
        We can verify every chunk on disk by rehashing and comparing.
        
        Actions:
        1. Scan chunks/ dir, verify hash matches filename
        2. Remove corrupt files from disk
        3. Remove DB entries for chunks missing from disk
        4. Remove orphaned ChunkLocations pointing to missing chunks
        
        This prevents silent corruption from becoming a data integrity problem
        when chunks are replicated to other nodes.
        """
        if not self.chunks_dir.exists():
            return
        
        print("Verifying chunk integrity on boot...")
        
        verified = 0
        corrupt = 0
        missing_from_disk = 0
        
        # Phase 1: Verify physical chunks match their content hash
        for dat_file in self.chunks_dir.glob("*.dat"):
            expected_hash = dat_file.stem
            if len(expected_hash) != 64:
                continue  # Not a valid chunk file
            
            try:
                actual_hash = await asyncio.to_thread(
                    lambda p=dat_file: hashlib.sha256(p.read_bytes()).hexdigest()
                )
                
                if actual_hash != expected_hash:
                    corrupt += 1
                    print(f"Corrupt chunk: {expected_hash[:12]}... (actual: {actual_hash[:12]}...)")
                    dat_file.unlink()
                    
                    # Remove DB records for this corrupt chunk
                    db = self.SessionLocal()
                    try:
                        db.query(ChunkLocation).filter(
                            ChunkLocation.chunk_hash == expected_hash,
                            ChunkLocation.node_id == self.node_id
                        ).delete()
                        db.query(Chunk).filter(Chunk.chunk_hash == expected_hash).delete()
                        db.commit()
                    finally:
                        db.close()
                else:
                    verified += 1
            except Exception as e:
                corrupt += 1
                print(f"Unreadable chunk: {expected_hash[:12]}... ({e})")
                try:
                    dat_file.unlink()
                except Exception:
                    pass
        
        # Phase 2: Check DB entries have corresponding files on disk
        db = self.SessionLocal()
        try:
            local_locations = db.query(ChunkLocation).filter(
                ChunkLocation.node_id == self.node_id
            ).all()
            
            for loc in local_locations:
                chunk_path = self.chunks_dir / f"{loc.chunk_hash}.dat"
                if not chunk_path.exists():
                    missing_from_disk += 1
                    db.delete(loc)
            
            if missing_from_disk > 0:
                db.commit()
        finally:
            db.close()
        
        if corrupt > 0 or missing_from_disk > 0:
            print(f" Integrity check: {verified} OK, {corrupt} corrupt (removed), {missing_from_disk} missing from disk (cleaned)")
        else:
            print(f"✓  Integrity check: {verified} chunks verified OK")
    
    
    async def _write_chunk_to_disk(
        self, 
        temp_id,
        chunk_index: int, 
        chunk_buffer: bytearray, 
        chunk_meta_list: list, 
        chunk_paths: list
    ):
        """
        Write a chunk to staging during streaming upload (NO DATABASE WRITES).
        
        Content-addressed: the chunk's SHA-256 hash IS its filename.
        Written to staging/{upload_id}/{hash}.dat, then moved to chunks/
        on commit. If chunks/{hash}.dat already exists, that's deduplication.
        
        Args:
            temp_id: Upload session ID — becomes staging subdirectory name
        """
        chunk_data = bytes(chunk_buffer)
        chunk_size = len(chunk_data)
        
        # Content hash = chunk identity
        chunk_checksum = hashlib.sha256(chunk_data).hexdigest()
        
        # Write to staging dir (non-blocking)
        stage_dir = self.staging_dir / temp_id
        stage_dir.mkdir(exist_ok=True, parents=True)
        chunk_path = stage_dir / f"{chunk_checksum}.dat"
        
        def write_chunk():
            with open(chunk_path, 'wb') as f:
                f.write(chunk_data)
        
        await asyncio.to_thread(write_chunk)
        
        # Verify chunk (read back and check hash)
        def verify_chunk():
            with open(chunk_path, 'rb') as f:
                written_data = f.read()
            return hashlib.sha256(written_data).hexdigest()
        
        written_checksum = await asyncio.to_thread(verify_chunk)
        if written_checksum != chunk_checksum:
            raise Exception(f"Chunk {chunk_index} verification failed: checksum mismatch after write")
        
        # Track metadata in memory (NOT in DB yet)
        chunk_meta_list.append({
            'chunk_index': chunk_index,
            'size_bytes': chunk_size,
            'chunk_hash': chunk_checksum,
        })
        chunk_paths.append(chunk_path)
        
        print(f" ✓ Chunk {chunk_index}: {chunk_size:,} bytes → {chunk_checksum[:12]}...")
        
        # Yield to event loop
        await asyncio.sleep(0)
    
    async def _get_http_session(self) -> aiohttp.ClientSession:
        """Get or create persistent aiohttp session for inter-node comms"""
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(
                    limit=50,
                    enable_cleanup_closed=True,
                ),
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._http_session
    
    def _update_capacity_cached(self):
        """Update capacity with caching to avoid DB queries inside locks"""
        now = time.time()
        if now - self._cached_capacity_time > self._capacity_cache_ttl:
            self._update_capacity()
            self._cached_capacity_time = now
    
    async def _reserve_space(self, size_gb: float) -> bool:
        """
        Atomically reserve space for an upload
        
        Returns: True if space reserved successfully, False if insufficient
        Uses cached capacity to avoid DB queries inside the lock.
        """
        # Update capacity OUTSIDE the lock to avoid deadlock with DB
        self._update_capacity_cached()
        
        async with self.reservation_lock:
            # Calculate truly available space (free - already reserved)
            truly_available = self.free_capacity_gb - self.reserved_space_gb
            
            if size_gb > truly_available:
                return False  # Insufficient space
            
            # Reserve the space
            self.reserved_space_gb += size_gb
            return True
    
    async def _release_reservation(self, size_gb: float):
        """
        Release a space reservation (called on upload failure)
        Refunds the reserved space back to the pool
        """
        async with self.reservation_lock:
            self.reserved_space_gb -= size_gb
            if self.reserved_space_gb < 0:
                self.reserved_space_gb = 0  # Safety check
    
    async def _commit_reservation(self, size_gb: float):
        """
        Commit a space reservation (called on upload success)
        Moves space from reserved to used
        """
        async with self.reservation_lock:
            self.reserved_space_gb -= size_gb
            if self.reserved_space_gb < 0:
                self.reserved_space_gb = 0  # Safety check
    
    async def _replicate_file_chunks(self, file_id: int):
        """
        Replicate file chunks to peer nodes with house-awareness
        - N=2 for clusters with 2-4 nodes
        - N=3 for clusters with 5+ nodes
        - Prefer nodes in different houses (different public IPs)
        
        Reads chunks from disk individually to avoid loading entire file into memory.
        Queries chunk records from a fresh DB session to avoid detached ORM objects.
        """
        try:
            import base64
            
            if not self.peer_nodes:
                print("No peers available for replication")
                return
            
            # Calculate replication factor based on cluster size
            total_nodes = 1 + len(self.peer_nodes)  # Self + peers
            if total_nodes <= 4:
                REPLICATION_FACTOR = min(2, len(self.peer_nodes))  # N=2 for small clusters
            else:
                REPLICATION_FACTOR = min(3, len(self.peer_nodes))  # N=3 for large clusters
            
            print(f" Starting replication (factor: {REPLICATION_FACTOR}, cluster size: {total_nodes})")
            
            # Select target nodes with house-awareness
            target_nodes = self._select_replica_nodes(REPLICATION_FACTOR)
            
            if not target_nodes:
                print("No suitable replica nodes found")
                return
            
            # Get file metadata AND chunk records from a fresh DB session.
            # The chunk_records passed in may be detached ORM objects from a closed
            # session (since this runs as a background task after upload completes).
            # Re-querying avoids SQLAlchemy's "not bound to a Session" error.
            db = self.SessionLocal()
            try:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file_record:
                    print(f" File {file_id} not found in database")
                    return
                
                file_metadata = {
                    'filename': file_record.filename,
                    'total_size_bytes': file_record.total_size_bytes,
                    'chunk_size_bytes': file_record.chunk_size_bytes,
                    'total_chunks': file_record.total_chunks,
                    'checksum_sha256': file_record.checksum_sha256,
                    'uploaded_by': file_record.uploaded_by
                }
                
                # Convert ORM chunk records to plain dicts (session-independent)
                fresh_chunks = db.query(FileChunk).filter(
                    FileChunk.file_id == file_id
                ).order_by(FileChunk.chunk_index).all()
                
                chunk_records = [
                    {'chunk_index': c.chunk_index, 'chunk_hash': c.chunk_hash}
                    for c in fresh_chunks
                ]
            finally:
                db.close()
            
            if not chunk_records:
                print(f" No chunks found for file {file_id}")
                return
            
            CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB
            
            # Replicate to each target node
            for target_node_id, target_info in target_nodes:
                target_url = f"http://{target_info['ip_address']}:{target_info['port']}"
                print(f"Replicating to: {target_info['node_name']} ({target_info['ip_address']})")
                
                # TCP-LIKE REPLICATION: Track ACKs and retry failed chunks
                MAX_RETRIES = 3
                RETRY_DELAY_BASE = 2  # seconds, exponentially increases
                
                # Track chunks: {chunk_index: {'sent': bool, 'acked': bool, 'retries': int}}
                chunk_status = {
                    chunk['chunk_index']: {'sent': False, 'acked': False, 'retries': 0}
                    for chunk in chunk_records
                }
                
                # Initial send: Try to send all chunks (streaming from disk)
                print(f"Initial send: {len(chunk_records)} chunks to {target_info['node_name']}")
                for i, chunk in enumerate(chunk_records):
                    success = await self._send_chunk_from_disk(
                        file_id, chunk, chunk_records,
                        file_metadata,  # Send with EVERY chunk — receiver deduplicates
                        target_url, target_info, chunk_status, CHUNK_SIZE
                    )
                    
                    # Yield to event loop after each chunk (allows heartbeats)
                    await asyncio.sleep(0)
                
                # Retry loop: Resend failed chunks with exponential backoff
                for retry_round in range(MAX_RETRIES):
                    # Find chunks that need retry
                    failed_chunks = [
                        chunk for chunk in chunk_records
                        if not chunk_status[chunk['chunk_index']]['acked'] 
                        and chunk_status[chunk['chunk_index']]['retries'] < MAX_RETRIES
                    ]
                    
                    if not failed_chunks:
                        break  # All chunks ACKed!
                    
                    # Exponential backoff before retry
                    retry_delay = RETRY_DELAY_BASE ** (retry_round + 1)
                    print(f"Retry round {retry_round + 1}/{MAX_RETRIES}: {len(failed_chunks)} chunks failed, waiting {retry_delay}s...")
                    await asyncio.sleep(retry_delay)
                    
                    # Retry failed chunks
                    for chunk in failed_chunks:
                        chunk_status[chunk['chunk_index']]['retries'] += 1
                        
                        success = await self._send_chunk_from_disk(
                            file_id, chunk, chunk_records,
                            file_metadata,  # Always include — ensures file record exists
                            target_url, target_info, chunk_status, CHUNK_SIZE
                        )
                        
                        await asyncio.sleep(0)
                
                # Final status check
                acked_count = sum(1 for s in chunk_status.values() if s['acked'])
                failed_count = len(chunk_records) - acked_count
                
                if failed_count > 0:
                    print(f"Replication incomplete to {target_info['node_name']}: {acked_count}/{len(chunk_records)} chunks ACKed, {failed_count} failed after {MAX_RETRIES} retries")
                else:
                    print(f"Replication complete to {target_info['node_name']}: {acked_count}/{len(chunk_records)} chunks ACKed")
            
            print(f"All replications complete: {len(chunk_records)} chunks → {len(target_nodes)} nodes")
            
            # Mark file as replicated in database
            db = self.SessionLocal()
            try:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if file_record and not file_record.is_replicated:
                    file_record.is_replicated = True
                    await db_commit_with_retry(db)
                    print(f"File {file_id} marked as replicated")
            except Exception as e:
                print(f"Error marking file as replicated: {e}")
            finally:
                db.close()
        
        except Exception as e:
            print(f"Replication failed: {e}")
    
    async def _send_chunk_from_disk(
        self, file_id: int, chunk: dict, chunk_records: list,
        file_metadata: dict, target_url: str, target_info: dict, 
        chunk_status: dict, CHUNK_SIZE: int
    ) -> bool:
        """
        Read a chunk from disk and send it with ACK verification.
        
        Reads from content-addressed chunks/ directory by hash.
        Uses persistent aiohttp session for connection reuse.
        
        Args:
            chunk: Plain dict with 'chunk_index' and 'chunk_hash' keys
        
        Returns:
            True if chunk was ACKed, False otherwise
        """
        idx = chunk['chunk_index']
        checksum = chunk['chunk_hash']
        
        try:
            import base64
            
            # Acquire replication slot — limits total chunks in-flight across all tasks.
            # Without this, multiple files finishing at once flood the peer with 24+
            # concurrent 64MB transfers, causing I/O saturation and health check timeouts.
            async with self._replication_semaphore:
                # Read chunk data from content-addressed store
                chunk_path = self.chunks_dir / f"{checksum}.dat"
                if not chunk_path.exists():
                    print(f"✗ Chunk {idx} not found: {checksum[:12]}...")
                    return False
                
                chunk_data = await asyncio.to_thread(chunk_path.read_bytes)
                
                # Encode as base64
                chunk_data_b64 = base64.b64encode(chunk_data).decode('utf-8')
                
                payload = {
                    'file_id': file_id,
                    'chunk_index': idx,
                    'chunk_data': chunk_data_b64,
                    'checksum': checksum,
                    'node_name': self.config.node_name,
                    'timestamp': time.time()
                }
                
                # Include file metadata if provided (first chunk)
                if file_metadata:
                    payload['file_metadata'] = file_metadata
                
                # SECURITY: Sign the payload
                signed_payload = self.identity.sign_json(payload)
                
                # Send to peer using persistent session (60s timeout for 64MB + DB write)
                session = await self._get_http_session()
                async with session.post(
                    f"{target_url}/api/internal/replicate_chunk",
                    json=signed_payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status == 200:
                        # Parse ACK response
                        ack_data = await response.json()
                        
                        # Verify ACK
                        if (ack_data.get('status') == 'ack' and 
                            ack_data.get('chunk_index') == idx and
                            ack_data.get('checksum') == checksum):
                            
                            # Mark as ACKed
                            chunk_status[idx]['sent'] = True
                            chunk_status[idx]['acked'] = True
                            print(f"✓ Chunk {idx + 1}/{len(chunk_records)} ACKed → {target_info['node_name']}")
                            return True
                        else:
                            print(f"✗ Chunk {idx} invalid ACK response")
                            return False
                    
                    elif response.status == 401:
                        print(f"✗ Chunk {idx} rejected: Invalid signature")
                        return False
                    else:
                        print(f"✗ Chunk {idx} failed: HTTP {response.status}")
                        return False
        
        except Exception as e:
            print(f"✗ Chunk {idx} error: {e}")
            return False
    
    def _select_replica_nodes(self, N: int) -> list:
        """
        Select N replica nodes using GREEDY strategy: pick nodes with most free space
        
        This ensures even distribution across the cluster by always selecting
        the nodes that currently have the most available storage.
        
        Ties are broken by latency (prefer distant nodes for better fault tolerance):
        - FAR:    >50ms  - Different region (best diversity)
        - MEDIUM: 5-50ms - Same city/region (good diversity)
        - CLOSE:  <5ms   - Same LAN/house (last resort)
        """
        if not self.peer_nodes:
            return []
        
        # Get online peers only
        online_peers = []
        for node_id, node_info in self.peer_nodes.items():
            if self.health_monitor:
                status = self.health_monitor.get_peer_status(node_id)
                if status == "online":
                    online_peers.append((node_id, node_info))
            else:
                online_peers.append((node_id, node_info))
        
        if not online_peers:
            return []
        
        # GREEDY STRATEGY: Sort by free space (descending), then by latency (DESCENDING for ties)
        def sort_key(peer):
            node_id, node_info = peer
            
            # Primary: Free space (higher is better)
            # Use cached free_capacity_gb if available, otherwise assume total storage is free
            free_gb = node_info.get('free_capacity_gb', node_info.get('storage_gb', 0))
            
            # Secondary: Latency (HIGHER is better for ties - prefer distant nodes)
            # Prefer far nodes (high latency) for better fault tolerance
            latency = node_info.get('latency_ms', 0)
            
            # Return tuple: (free_space desc, latency desc)
            # Negate both for descending sort - prefers high latency when free space is equal
            return (-free_gb, -latency)
        
        sorted_peers = sorted(online_peers, key=sort_key)
        
        # Select top N nodes
        selected = sorted_peers[:N]
        
        if len(selected) < N:
            print(f"Only {len(selected)} replica nodes available (wanted {N})")
        
        # Log selection details
        self._log_replica_selection(selected)
        
        return selected
    
    def _log_replica_selection(self, selected_replicas: list):
        """Log information about selected replica nodes"""
        if not selected_replicas:
            return
        
        print(f"Selected {len(selected_replicas)} replica nodes:")
        
        # Collect latencies and categorize nodes
        latencies = []
        far_count = 0
        medium_count = 0
        close_count = 0
        unknown_count = 0
        
        for node_id, node_info in selected_replicas:
            free_gb = node_info.get('free_capacity_gb', node_info.get('storage_gb', 0))
            total_gb = node_info.get('storage_gb', 0)
            latency = node_info.get('latency_ms', 0)
            utilization = ((total_gb - free_gb) / total_gb * 100) if total_gb > 0 else 0
            
            print(f" • {node_info['node_name']}: {free_gb:.1f}GB free / {total_gb:.1f}GB total ({utilization:.1f}% used), {latency:.1f}ms latency")
            
            # Categorize by latency
            if latency > 50:
                far_count += 1
            elif latency >= 5:
                medium_count += 1
            elif latency > 0:
                close_count += 1
            else:
                unknown_count += 1
            
            if latency > 0:
                latencies.append(latency)
        
        # Build diversity report
        diversity_parts = []
        if far_count > 0:
            diversity_parts.append(f"{far_count} distant (>50ms)")
        if medium_count > 0:
            diversity_parts.append(f"{medium_count} regional (5-50ms)")
        if close_count > 0:
            diversity_parts.append(f"{close_count} local (<5ms)")
        if unknown_count > 0:
            diversity_parts.append(f"{unknown_count} unknown")
        
        diversity_str = ", ".join(diversity_parts)
        
        if far_count > 0:
            print(f"Geographic diversity: {diversity_str}")
        elif medium_count > 0:
            print(f"Regional diversity: {diversity_str}")
        elif close_count > 0:
            print(f"Local replicas only: {diversity_str} - consider adding remote nodes")
        else:
            print(f"Replica distribution: {diversity_str}")
        
        # Show actual latencies for transparency
        if latencies:
            min_lat = min(latencies)
            max_lat = max(latencies)
            avg_lat = sum(latencies) / len(latencies)
            print(f" Latency range: {min_lat:.1f}ms - {max_lat:.1f}ms (avg: {avg_lat:.1f}ms)")
    
    async def _decrement_assignment(self, node_id: str = "self"):
        """Decrement the assignment counter when an upload completes."""
        async with self._assigned_uploads_lock:
            if node_id in self._assigned_uploads:
                self._assigned_uploads[node_id] = max(0, self._assigned_uploads[node_id] - 1)
    
    async def _notify_leader_upload_complete(self):
        """
        Tell the leader that a delegated upload finished on this node.
        Called after a delegated upload completes so the leader's load
        balancer counter stays accurate.
        """
        if not self.raft or self.raft.is_leader():
            return  # We ARE the leader, decrement locally
        
        leader_id = self.raft.get_leader_id()
        if not leader_id or leader_id not in self.peer_nodes:
            return
        
        peer = self.peer_nodes[leader_id]
        leader_url = f"http://{peer['ip_address']}:{peer['port']}"
        
        try:
            session = await self._get_http_session()
            async with session.post(
                f"{leader_url}/api/upload/complete_notify",
                json={"node_id": self.node_id},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                pass  # Fire and forget — counter will self-correct anyway
        except Exception:
            pass  # Not critical — health check will sync the counter
    
    async def _reconcile_on_promotion(self):
        """
        METADATA RECONCILIATION — run when this node becomes leader.
        
        When a leader dies, the new leader's DB may be missing metadata for
        files that were:
        - Uploaded directly to the old leader (not yet replicated)
        - Assigned via delegation but only partially replicated
        - In-flight when the old leader crashed
        
        Since files on disk are the real source of truth, we rebuild our
        metadata by querying every peer's inventory and merging it with
        our own database.
        
        Steps:
        1. Clean stale delegation placeholders
        2. Scan local disk for orphaned chunks
        3. Query each peer's /api/inventory
        4. Import metadata for files we don't know about
        5. Trigger re-replication for under-replicated files
        """
        print()
        print("=" * 60)
        print("METADATA RECONCILIATION — new leader recovery")
        print("=" * 60)
        
        # Allow cluster to stabilize (peers need to recognize new leader)
        await asyncio.sleep(3.0)
        
        if not self.raft or not self.raft.is_leader():
            print("Lost leadership during reconciliation wait — aborting")
            return
        
        # ========================================
        # STEP 1: Clean stale delegation placeholders
        # ========================================
        async with self._db_write_semaphore:
            db = self.SessionLocal()
            try:
                stale = db.query(FileModel).filter(
                    FileModel.checksum_sha256 == "pending_delegation"
                ).all()
                
                if stale:
                    stale_ids = [f.id for f in stale]
                    for f in stale:
                        db.delete(f)
                    await db_commit_with_retry(db)
                    print(f"Step 1: Cleaned {len(stale)} stale delegation placeholders (IDs: {stale_ids})")
                else:
                    print(f"Step 1: No stale delegation placeholders")
            except Exception as e:
                db.rollback()
                print(f"Step 1 failed: {e}")
            finally:
                db.close()
        
        # ========================================
        # STEP 2: Scan local disk for orphaned chunks
        # ========================================
        orphaned_hashes = []
        
        if self.chunks_dir.exists():
            db = self.SessionLocal()
            try:
                for dat_file in self.chunks_dir.glob("*.dat"):
                    chunk_hash = dat_file.stem
                    if len(chunk_hash) == 64:
                        # Check if any file references this chunk
                        refs = db.query(FileChunk).filter(
                            FileChunk.chunk_hash == chunk_hash
                        ).count()
                        if refs == 0:
                            orphaned_hashes.append(chunk_hash)
            finally:
                db.close()
        
        if orphaned_hashes:
            print(f"Step 2: Found {len(orphaned_hashes)} orphaned chunks on local disk (unreferenced by any file)")
        else:
            print(f"✓  Step 2: No orphaned chunks on local disk")
        
        # ========================================
        # STEP 3: Query peer inventories
        # ========================================
        peer_inventories = {}  # node_id -> inventory response
        
        for node_id, peer_info in list(self.peer_nodes.items()):
            if self.health_monitor:
                status = self.health_monitor.get_peer_status(node_id)
                # Also check heartbeat thread's liveness
                if status != "online" and not (self.raft and self.raft.is_peer_alive(node_id)):
                    continue
            
            peer_url = f"http://{peer_info['ip_address']}:{peer_info['port']}"
            try:
                session = await self._get_http_session()
                async with session.get(
                    f"{peer_url}/api/inventory",
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        inv = await resp.json()
                        peer_inventories[node_id] = inv
                        file_count = len(inv.get("files", []))
                        print(f"Step 3: {peer_info['node_name']} reports {file_count} files")
                    else:
                        print(f"Step 3: {peer_info['node_name']} returned HTTP {resp.status}")
            except Exception as e:
                print(f"Step 3: Failed to reach {peer_info['node_name']}: {e}")
        
        if not peer_inventories:
            print("Step 3: No peer inventories available — reconciliation limited to local data")
        
        # ========================================
        # STEP 4: Import metadata for unknown files
        # ========================================
        files_imported = 0
        files_already_known = 0
        
        for node_id, inventory in peer_inventories.items():
            peer_name = inventory.get("node_name", node_id[:8])
            
            for file_info in inventory.get("files", []):
                file_id = file_info["file_id"]
                
                # Skip delegation placeholders on peers
                if file_info.get("checksum_sha256") == "pending_delegation":
                    continue
                
                # Skip incomplete files (partial uploads that never finished)
                if not file_info.get("is_complete", False):
                    continue
                
                # Check if we already know about this file
                db = self.SessionLocal()
                try:
                    existing = db.query(FileModel).filter(FileModel.id == file_id).first()
                    
                    if existing and existing.checksum_sha256 != "pending_delegation":
                        files_already_known += 1
                        continue
                    
                    # Either doesn't exist or is a stale placeholder — create/update
                    async with self._db_write_semaphore:
                        db2 = self.SessionLocal()
                        try:
                            existing2 = db2.query(FileModel).filter(FileModel.id == file_id).first()
                            
                            if existing2:
                                # Update placeholder with real data
                                existing2.filename = file_info["filename"]
                                existing2.total_size_bytes = file_info["total_size_bytes"]
                                existing2.chunk_size_bytes = file_info.get("chunk_size_bytes", 64 * 1024 * 1024)
                                existing2.total_chunks = file_info["total_chunks"]
                                existing2.checksum_sha256 = file_info["checksum_sha256"]
                                existing2.uploaded_by = file_info.get("uploaded_by", "reconciled")
                                existing2.is_complete = True
                                existing2.is_replicated = False  # Will trigger re-replication
                            else:
                                new_file = FileModel(
                                    id=file_id,
                                    filename=file_info["filename"],
                                    total_size_bytes=file_info["total_size_bytes"],
                                    chunk_size_bytes=file_info.get("chunk_size_bytes", 64 * 1024 * 1024),
                                    total_chunks=file_info["total_chunks"],
                                    checksum_sha256=file_info["checksum_sha256"],
                                    uploaded_by=file_info.get("uploaded_by", "reconciled"),
                                    is_complete=True,
                                    is_replicated=False,
                                )
                                db2.add(new_file)
                            
                            await db_flush_with_retry(db2)
                            
                            # Import FileChunk mappings from peer's inventory.
                            # Without these, the file exists in metadata but has no
                            # chunk references — it can't be downloaded or replicated.
                            for chunk_info in file_info.get("chunks_on_disk", []):
                                # Ensure canonical Chunk record exists
                                self._ensure_chunk(
                                    db2, chunk_info["chunk_hash"],
                                    chunk_info["size_bytes"]
                                )
                                
                                existing_fc = db2.query(FileChunk).filter(
                                    FileChunk.file_id == file_id,
                                    FileChunk.chunk_index == chunk_info["chunk_index"]
                                ).first()
                                
                                if not existing_fc:
                                    fc = FileChunk(
                                        file_id=file_id,
                                        chunk_index=chunk_info["chunk_index"],
                                        chunk_hash=chunk_info["chunk_hash"],
                                    )
                                    db2.add(fc)
                            
                            await db_commit_with_retry(db2)
                            files_imported += 1
                            chunk_count = len(file_info.get("chunks_on_disk", []))
                            print(f"Imported: {file_info['filename']} (ID: {file_id}, {chunk_count} chunks) from {peer_name}")
                        except Exception as e:
                            db2.rollback()
                            print(f"Failed to import file {file_id}: {e}")
                        finally:
                            db2.close()
                finally:
                    db.close()
        
        print(f"✓  Step 4: Imported {files_imported} files from peers ({files_already_known} already known)")
        
        # ========================================
        # STEP 5: Trigger re-replication for files only on one node
        # ========================================
        if len(self.peer_nodes) > 0:
            # Wait a moment for DB writes to settle
            await asyncio.sleep(1.0)
            
            replication_needed = 0
            db = self.SessionLocal()
            try:
                # Files marked as not replicated
                unreplicated = db.query(FileModel).filter(
                    FileModel.is_complete == True,
                    FileModel.is_replicated == False,
                ).all()
                
                for f in unreplicated:
                    # Check if we have the chunks PHYSICALLY on disk (not just in DB).
                    # After reconciliation, we may have imported FileChunk mappings from
                    # a peer but don't have the actual chunk data locally.
                    file_chunks = db.query(FileChunk).filter(FileChunk.file_id == f.id).all()
                    local_on_disk = sum(
                        1 for fc in file_chunks
                        if (self.chunks_dir / f"{fc.chunk_hash}.dat").exists()
                    )
                    has_local = local_on_disk == f.total_chunks
                    
                    if has_local:
                        # We have the file — replicate to peers
                        asyncio.create_task(self._replicate_file_chunks(f.id))
                        replication_needed += 1
                    else:
                        # File is on a peer but not here — request it via redistribution
                        # This will be handled by _redistribute_existing_files
                        replication_needed += 1
            finally:
                db.close()
            
            if replication_needed > 0:
                print(f"Step 5: Triggered replication for {replication_needed} under-replicated files")
            else:
                print(f"Step 5: All files properly replicated")
        else:
            print(f"Step 5: No peers — skipping replication check")
        
        print("=" * 60)
        print("RECONCILIATION COMPLETE")
        print("=" * 60)
        print()
    
    async def _redistribute_existing_files(self):
        """
        Called when a new node joins the cluster to replicate existing files
        
        This ensures that files uploaded when the cluster had only 1 node
        get replicated once a second node joins.
        """
        if not self.raft or not self.raft.is_leader():
            print("Not leader, skipping redistribution")
            return
        
        if len(self.peer_nodes) == 0:
            print("No peers available for redistribution")
            return
        
        db = self.SessionLocal()
        try:
            # Find files that need more replicas
            files_needing_replication = db.execute(
                text("""
                    SELECT 
                        f.id,
                        f.filename,
                        COUNT(DISTINCT cl.node_id) as current_replicas,
                        f.total_chunks
                    FROM files f
                    JOIN file_chunks fc ON fc.file_id = f.id
                    LEFT JOIN chunk_locations cl ON cl.chunk_hash = fc.chunk_hash
                    WHERE f.is_complete = 1
                    GROUP BY f.id, f.filename, f.total_chunks
                """)
            ).fetchall()
            
            # Determine target replication factor
            total_nodes = 1 + len(self.peer_nodes)
            if total_nodes <= 4:
                target_replication = 2
            else:
                target_replication = 3
            
            files_to_replicate = []
            for file_id, filename, current_replicas, total_chunks in files_needing_replication:
                # Check if file needs more replication
                needs_replication = current_replicas < target_replication
                
                # Also check database flag to avoid re-replicating
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                already_replicated = file_record.is_replicated if file_record else False
                
                # Only replicate if under-replicated AND not already marked as replicated
                # (OR if marked as replicated but current count is 0, meaning replicas were lost)
                if needs_replication and (not already_replicated or current_replicas == 0):
                    files_to_replicate.append((file_id, filename, current_replicas, total_chunks))
                elif already_replicated and current_replicas >= target_replication:
                    # File is properly replicated, skip
                    pass
                elif already_replicated and needs_replication:
                    print(f"File {filename} marked as replicated but only has {current_replicas}/{target_replication} replicas (likely node went offline)")
            
            if not files_to_replicate:
                print("✓ All files properly replicated")
                return
            
            print(f"Redistributing {len(files_to_replicate)} files to new cluster topology...")
            
            for file_id, filename, current_replicas, total_chunks in files_to_replicate:
                print(f"Replicating: {filename} (currently {current_replicas} replicas, want {target_replication})")
                
                # Get chunk records (content-addressed)
                chunks = db.query(FileChunk).filter(
                    FileChunk.file_id == file_id
                ).order_by(FileChunk.chunk_index).all()
                
                if not chunks:
                    print(f"No chunks found for file {file_id}")
                    continue
                
                try:
                    # Trigger replication (reads chunks from disk, no memory loading)
                    await self._replicate_file_chunks(file_id)
                    
                    print(f"Replication scheduled for {filename}")
                except Exception as e:
                    print(f"Failed to replicate {filename}: {e}")
            
            print(f"Redistribution complete")
            
        except Exception as e:
            print(f"Redistribution failed: {e}")
        finally:
            db.close()
    
    async def _proxy_upload_to_leader(self, file: UploadFile, leader_id: str):
        """Proxy an upload request to the leader node"""
        leader_info = self.peer_nodes[leader_id]
        leader_url = f"http://{leader_info['ip_address']}:{leader_info['port']}/api/upload"
        
        print(f"Proxying upload to leader {leader_info['node_name']}...")
        
        # Read file content
        file_content = await file.read()
        
        # Create form data
        form_data = aiohttp.FormData()
        form_data.add_field(
            'file',
            file_content,
            filename=file.filename,
            content_type=file.content_type or 'application/octet-stream'
        )
        
        # Forward to leader
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    leader_url,
                    data=form_data,
                    timeout=aiohttp.ClientTimeout(total=300)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        print(f" ✓ Upload proxied successfully")
                        return result
                    else:
                        error_text = await response.text()
                        raise HTTPException(
                            status_code=response.status,
                            detail=f"Leader upload failed: {error_text}"
                        )
        except aiohttp.ClientError as e:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to reach leader: {str(e)}"
            )
    
    def _setup_routes(self):
        """Setup FastAPI routes"""
        
        # Serve frontend
        frontend_dir = Path(__file__).parent.parent / "frontend"
        if frontend_dir.exists():
            self.app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")
            
            @self.app.get("/")
            async def root():
                from fastapi.responses import HTMLResponse
                html_path = frontend_dir / "index.html"
                if html_path.exists():
                    content = html_path.read_text()
                    return HTMLResponse(
                        content=content,
                        headers={
                            "Cache-Control": "no-cache, no-store, must-revalidate",
                            "Pragma": "no-cache",
                            "Expires": "0"
                        }
                    )
                return FileResponse(html_path)
        
        # Add cache-control headers to API endpoints
        @self.app.middleware("http")
        async def add_no_cache_headers(request, call_next):
            response = await call_next(request)
            if request.url.path.startswith("/api/"):
                response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                response.headers["Pragma"] = "no-cache"
                response.headers["Expires"] = "0"
            return response
        
        # Cluster info endpoint
        @self.app.get("/api/cluster/info")
        async def get_cluster_info():
            return {
                "cluster_id": self.config.cluster_id,
                "node_id": self.config.node_id,
                "node_name": self.config.node_name,
                "is_leader": self.raft.is_leader() if self.raft else self.is_leader,
                "leader_id": self.raft.get_leader_id() if self.raft else self.config.node_id,
                "raft_state": self.raft.get_state() if self.raft else "unknown",
                "raft_enabled": self.raft is not None
            }
        
        @self.app.get("/api/upload/status")
        async def get_upload_status():
            """Show upload queue status — how many slots are available/in use"""
            max_concurrent = self._upload_semaphore._value + (3 - self._upload_semaphore._value)
            active = 3 - self._upload_semaphore._value  # Semaphore initialized to 3
            return {
                "max_concurrent_uploads": 3,
                "active_uploads": active,
                "available_slots": self._upload_semaphore._value,
                "reserved_space_gb": round(self.reserved_space_gb, 2),
                "free_capacity_gb": round(self.free_capacity_gb, 2),
            }
        
        # Raft consensus endpoints
        @self.app.get("/api/raft/ping")
        async def raft_ping():
            """
            ZERO-I/O liveness probe for pre-vote checks.
            
            Unlike /api/health which calls _update_capacity() (disk I/O),
            this returns instantly. Used by followers to check if the leader
            is alive before starting an election. Under heavy upload I/O,
            /api/health can take >5s to respond, causing false "leader dead"
            conclusions. This endpoint avoids that entirely.
            """
            return {"alive": True, "node_id": self.node_id, "t": time.time()}
        
        @self.app.post("/api/raft/heartbeat")
        async def receive_raft_heartbeat(request: dict):
            """Receive heartbeat from leader (with signature verification)"""
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")
            
            # SECURITY: Verify signature (with backward compatibility)
            if 'signature' in request and 'public_key' in request:
                # New secure format - verify signature
                verified, message, sender_node_id = self.trust_store.verify_message(request)
                
                if not verified:
                    print(f"Rejected heartbeat: Invalid signature")
                    raise HTTPException(401, "Invalid signature")
                
                # Extract verified data
                leader_id = sender_node_id
                term = message['term']
                leader_name = message.get('node_name', 'unknown')
                
                print(f"Verified heartbeat from {leader_name} ({sender_node_id[:8]}...)")
            else:
                # Old insecure format (temporary backward compatibility)
                leader_id = request.get('leader_id')
                term = request.get('term')
                
                if not leader_id or term is None:
                    raise HTTPException(400, "Missing leader_id or term")
                
                print(f"Accepting UNSIGNED heartbeat from {leader_id} (legacy mode)")
            
            await self.raft.receive_heartbeat(leader_id, term)
            return {"status": "ok", "follower_term": self.raft.current_term}
        
        @self.app.post("/api/raft/vote")
        async def receive_raft_vote_request(request: dict):
            """Receive vote request from candidate (with signature verification)"""
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")
            
            # SECURITY: Verify signature (with backward compatibility)
            if 'signature' in request and 'public_key' in request:
                # New secure format - verify signature
                verified, message, sender_node_id = self.trust_store.verify_message(request)
                
                if not verified:
                    print(f"Rejected vote request: Invalid signature")
                    raise HTTPException(401, "Invalid signature")
                
                # Extract verified data
                candidate_id = sender_node_id
                term = message['term']
                candidate_name = message.get('node_name', 'unknown')
                
                print(f"Verified vote request from {candidate_name} ({sender_node_id[:8]}...)")
            else:
                # Old insecure format (temporary backward compatibility)
                candidate_id = request.get('candidate_id')
                term = request.get('term')
                
                if not candidate_id or term is None:
                    raise HTTPException(400, "Missing candidate_id or term")
                
                print(f"Accepting UNSIGNED vote request from {candidate_id} (legacy mode)")
            
            vote_granted = await self.raft.request_vote(candidate_id, term)
            return {"vote_granted": vote_granted}
        
        # Upload delegation endpoint
        @self.app.get("/api/upload/assign")
        async def assign_upload(filename: str, size: int):
            """
            PROACTIVE UPLOAD LOAD BALANCER
            
            Every upload goes through this endpoint first. The leader distributes
            uploads evenly across ALL nodes in the cluster, not just when it's
            overloaded. This prevents any single node from getting saturated.
            
            Algorithm:
            1. Count assigned-but-not-completed uploads per node (including self)
            2. Pick the node with the fewest active uploads
            3. Tie-break: prefer non-leader nodes (leader has Raft overhead)
            4. If target is a peer, pre-allocate file_id for coordination
            
            Example with 2 nodes (leader=node1):
              File 1: node1=0, node2=0 → node2 (prefer non-leader on tie)
              File 2: node1=0, node2=1 → node1
              File 3: node1=1, node2=1 → node2 (prefer non-leader on tie)
              File 4: node1=1, node2=2 → node1
              ...evenly distributed!
            """
            my_url = f"http://{self.local_ip}:{self.config.port}"
            
            # Only the leader can assign uploads
            is_current_leader = self.raft and self.raft.is_leader()
            if not is_current_leader:
                # Follower — redirect client to the leader for assignment
                leader_id = self.raft.get_leader_id() if self.raft else None
                if leader_id and leader_id in self.peer_nodes:
                    peer = self.peer_nodes[leader_id]
                    leader_url = f"http://{peer['ip_address']}:{peer['port']}"
                    return {"upload_to": leader_url, "redirect_to_leader": True, "delegated": False, "file_id": None}
                raise HTTPException(503, "No leader available - cluster electing")
            
            size_gb = size / (1024 ** 3)
            
            async with self._assigned_uploads_lock:
                # Build candidate list: self + all online peers
                candidates = []
                
                # Self (leader)
                my_active = self._assigned_uploads.get("self", 0)
                self._update_capacity_cached()
                my_free_gb = self.free_capacity_gb - self.reserved_space_gb
                if my_free_gb >= size_gb:
                    candidates.append({
                        "id": "self",
                        "name": self.config.node_name,
                        "url": my_url,
                        "active": my_active,
                        "free_gb": my_free_gb,
                        "is_leader": True,
                    })
                
                # Peers
                for node_id, node_info in self.peer_nodes.items():
                    # Only consider online peers
                    if self.health_monitor:
                        status = self.health_monitor.get_peer_status(node_id)
                        if status != "online":
                            continue
                    
                    peer_free = node_info.get('free_capacity_gb', 0)
                    if peer_free < size_gb:
                        continue  # Not enough space
                    
                    peer_active = self._assigned_uploads.get(node_id, 0)
                    
                    candidates.append({
                        "id": node_id,
                        "name": node_info.get('node_name', 'unknown'),
                        "url": f"http://{node_info['ip_address']}:{node_info['port']}",
                        "active": peer_active,
                        "free_gb": peer_free,
                        "is_leader": False,
                    })
                
                if not candidates:
                    raise HTTPException(507, "No nodes with sufficient storage available")
                
                # Sort: fewest active uploads first, then prefer non-leader on tie
                candidates.sort(key=lambda c: (c["active"], c["is_leader"]))
                winner = candidates[0]
                
                # Track the assignment
                self._assigned_uploads[winner["id"]] = winner["active"] + 1
            
            # If assigning to self, no delegation needed
            if winner["id"] == "self":
                total_cluster = sum(self._assigned_uploads.values())
                print(f"Assign '{filename}' → {winner['name']} (self, {winner['active']+1} active, cluster total: {total_cluster})")
                return {
                    "upload_to": my_url,
                    "delegated": False,
                    "file_id": None,
                    "assigned_node": winner["name"],
                }
            
            # Delegating to a peer — pre-allocate file_id
            async with self._db_write_semaphore:
                db = self.SessionLocal()
                try:
                    placeholder = FileModel(
                        filename=filename,
                        total_size_bytes=size,
                        chunk_size_bytes=64 * 1024 * 1024,
                        total_chunks=0,
                        checksum_sha256="pending_delegation",
                        uploaded_by="delegated",
                        is_complete=False,
                        is_replicated=False
                    )
                    db.add(placeholder)
                    await db_flush_with_retry(db)
                    file_id = placeholder.id
                    await db_commit_with_retry(db)
                except Exception:
                    db.rollback()
                    # Pre-allocation failed — fall back to self
                    return {"upload_to": my_url, "delegated": False, "file_id": None, "assigned_node": self.config.node_name}
                finally:
                    db.close()
            
            total_cluster = sum(self._assigned_uploads.values())
            print(f"Assign '{filename}' → {winner['name']} (file_id={file_id}, {winner['active']+1} active, cluster total: {total_cluster})")
            
            return {
                "upload_to": winner["url"],
                "delegated": True,
                "file_id": file_id,
                "assigned_node": winner["name"],
            }
        
        @self.app.post("/api/upload/complete_notify")
        async def upload_complete_notify(request: dict):
            """
            Called by nodes (or internally) when a delegated upload finishes.
            Decrements the assignment counter so the load balancer stays accurate.
            """
            node_id = request.get("node_id", "self")
            async with self._assigned_uploads_lock:
                if node_id in self._assigned_uploads:
                    self._assigned_uploads[node_id] = max(0, self._assigned_uploads[node_id] - 1)
            return {"status": "ok"}
        
        # Upload endpoint
        @self.app.post("/api/upload")
        async def upload_file(
            file: UploadFile = FastAPIFile(...),
            delegated_file_id: Optional[int] = Query(None, description="Pre-assigned file ID from leader for delegated uploads")
        ):
            """
            STREAMING Upload with constant memory usage
            
            ARCHITECTURE:
            - Phase 1: Stream to disk (NO DATABASE - fully parallel)
            - Phase 2: Batch DB write under semaphore (<100ms lock)
            - Phase 3: Rename temp files to permanent names
            - Memory usage: ~64MB constant (regardless of file size!)
            
            Supports delegated uploads: when the leader's upload slots are full,
            it pre-assigns a file_id and directs the client here. The delegated_file_id
            param bypasses the leader check and uses the pre-assigned ID.
            """
            is_delegated = delegated_file_id is not None
            
            if is_delegated:
                print(f"Delegated upload received: {file.filename} (file_id={delegated_file_id})")
            else:
                # Check if we're the leader (with grace period for in-flight requests)
                # During heavy I/O, Raft elections can flip leadership temporarily.
                # A 30s grace period lets queued uploads complete rather than 503-ing
                # uploads that were accepted moments ago.
                LEADER_GRACE_PERIOD = 30  # seconds
                is_current_leader = self.raft and self.raft.is_leader()
                recently_was_leader = (
                    not is_current_leader and
                    self._leadership_lost_at > 0 and
                    (time.time() - self._leadership_lost_at) < LEADER_GRACE_PERIOD
                )
                
                if self.raft and not is_current_leader and not recently_was_leader:
                    leader_id = self.raft.get_leader_id()
                    if leader_id and leader_id in self.peer_nodes:
                        return await self._proxy_upload_to_leader(file, leader_id)
                    else:
                        raise HTTPException(503, "No leader available - cluster electing")
                
                if recently_was_leader:
                    print(f"Accepting upload during leadership grace period ({time.time() - self._leadership_lost_at:.1f}s since step-down)")
            
            # Constants
            CHUNK_SIZE = 64 * 1024 * 1024  # 64MB chunks
            READ_SIZE = 8192  # 8KB read buffer
            
            # Peek at file size
            file.file.seek(0, 2)
            file_size = file.file.tell()
            file.file.seek(0)
            file_size_gb = file_size / (1024 ** 3)
            
            # Enforce max upload size
            if file_size > MAX_UPLOAD_SIZE_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large: {file_size / (1024**3):.2f}GB exceeds "
                           f"max {MAX_UPLOAD_SIZE_BYTES / (1024**3):.0f}GB"
                )
            
            # Reserve space upfront
            space_reserved = await self._reserve_space(file_size_gb)
            if not space_reserved:
                self._update_capacity()
                available = self.free_capacity_gb - self.reserved_space_gb
                raise HTTPException(
                    status_code=507,
                    detail=f"Insufficient storage: need {file_size_gb:.2f}GB, available {available:.2f}GB"
                )
            
            # Temp upload ID for disk filenames (no DB yet)
            import uuid
            temp_id = uuid.uuid4().hex[:12]
            
            print(f"Upload accepted: {file.filename} ({file_size:,} bytes)")
            
            chunk_paths_temp = []  # Temp paths on disk
            chunk_meta_list = []   # In-memory metadata for batch DB insert
            
            try:
                # ========================================
                # PHASE 1: Stream to disk (concurrency-limited)
                # ========================================
                # Acquire upload slot - queues if too many concurrent uploads.
                # Space is already reserved, so queued uploads won't be rejected
                # for space when they eventually start streaming.
                async with self._upload_semaphore:
                    print(f"Streaming started: {file.filename}")
                    
                    chunk_index = 0
                    chunk_buffer = bytearray()
                    file_hash = hashlib.sha256()
                    total_bytes_read = 0
                    
                    while True:
                        piece = await file.read(READ_SIZE)
                        if not piece:
                            break
                        
                        file_hash.update(piece)
                        total_bytes_read += len(piece)
                        chunk_buffer.extend(piece)
                        
                        if len(chunk_buffer) >= CHUNK_SIZE:
                            await self._write_chunk_to_disk(
                                temp_id, chunk_index,
                                chunk_buffer, chunk_meta_list, chunk_paths_temp
                            )
                            chunk_index += 1
                            chunk_buffer.clear()
                            
                            if chunk_index % 10 == 0:
                                print(f"Progress: {chunk_index} chunks ({total_bytes_read / (1024**3):.2f} GB)")
                        
                        if total_bytes_read % (READ_SIZE * 100) == 0:
                            await asyncio.sleep(0)
                    
                    # Final partial chunk
                    if chunk_buffer:
                        await self._write_chunk_to_disk(
                            temp_id, chunk_index,
                            chunk_buffer, chunk_meta_list, chunk_paths_temp
                        )
                        chunk_index += 1
                
                # Upload semaphore released here — slot available for next upload
                
                file_checksum = file_hash.hexdigest()
                
                # ========================================
                # PHASE 2: Atomic DB commit (File + FileChunks + ChunkLocations)
                # ========================================
                async with self._db_write_semaphore:
                    db = self.SessionLocal()
                    try:
                        if is_delegated:
                            file_record = FileModel(
                                id=delegated_file_id,
                                filename=file.filename,
                                total_size_bytes=file_size,
                                chunk_size_bytes=CHUNK_SIZE,
                                total_chunks=chunk_index,
                                checksum_sha256=file_checksum,
                                uploaded_by="delegated",
                                is_complete=True,
                                is_replicated=False
                            )
                        else:
                            file_record = FileModel(
                                filename=file.filename,
                                total_size_bytes=file_size,
                                chunk_size_bytes=CHUNK_SIZE,
                                total_chunks=chunk_index,
                                checksum_sha256=file_checksum,
                                uploaded_by="web_user",
                                is_complete=True,
                                is_replicated=False
                            )
                        db.add(file_record)
                        await db_flush_with_retry(db)
                        
                        file_id = file_record.id
                        
                        for meta in chunk_meta_list:
                            # Canonical Chunk record (insert-if-not-exists, manages refcount)
                            self._ensure_chunk(db, meta['chunk_hash'], meta['size_bytes'])
                            
                            # FileChunk: ordered mapping from file → content-addressed chunk
                            file_chunk = FileChunk(
                                file_id=file_id,
                                chunk_index=meta['chunk_index'],
                                chunk_hash=meta['chunk_hash'],
                            )
                            db.add(file_chunk)
                            
                            # ChunkLocation: this node has this chunk
                            # Only add if not already tracked (dedup)
                            existing_loc = db.query(ChunkLocation).filter(
                                ChunkLocation.chunk_hash == meta['chunk_hash'],
                                ChunkLocation.node_id == self.node_id
                            ).first()
                            if not existing_loc:
                                location = ChunkLocation(
                                    chunk_hash=meta['chunk_hash'],
                                    node_id=self.node_id,
                                )
                                db.add(location)
                        
                        await db_commit_with_retry(db)
                    except Exception:
                        db.rollback()
                        raise
                    finally:
                        db.close()
                
                # ========================================
                # PHASE 3: Move staging → chunks/ (content-addressed)
                # ========================================
                for meta in chunk_meta_list:
                    staged_path = self.staging_dir / temp_id / f"{meta['chunk_hash']}.dat"
                    final_path = self.chunks_dir / f"{meta['chunk_hash']}.dat"
                    if final_path.exists():
                        # Dedup: chunk already exists from another upload
                        staged_path.unlink(missing_ok=True)
                    elif staged_path.exists():
                        await asyncio.to_thread(staged_path.rename, final_path)
                
                # Clean up staging directory
                stage_dir = self.staging_dir / temp_id
                if stage_dir.exists():
                    import shutil as _shutil
                    _shutil.rmtree(stage_dir, ignore_errors=True)
                
                await self._commit_reservation(file_size_gb)
                
                print(f"Upload complete: {file.filename}")
                print(f" File ID: {file_id}, Size: {total_bytes_read:,} bytes ({chunk_index} chunks)")
                print(f" Checksum: {file_checksum[:16]}...")
                
                # Update load balancer counter
                if is_delegated:
                    asyncio.create_task(self._notify_leader_upload_complete())
                else:
                    await self._decrement_assignment("self")
                
                # Replicate to peers if available
                if len(self.peer_nodes) > 0:
                    asyncio.create_task(self._replicate_file_chunks(file_id))
                
                return {
                    "message": "File uploaded successfully (streaming)",
                    "file_id": file_id,
                    "filename": file.filename,
                    "size_bytes": total_bytes_read,
                    "total_chunks": chunk_index,
                    "chunk_size_bytes": CHUNK_SIZE,
                    "checksum": file_checksum,
                    "is_complete": True
                }
            
            except HTTPException:
                await self._release_reservation(file_size_gb)
                # Clean staging dir on failure
                stage_dir = self.staging_dir / temp_id
                if stage_dir.exists():
                    import shutil as _shutil
                    _shutil.rmtree(stage_dir, ignore_errors=True)
                if is_delegated:
                    asyncio.create_task(self._notify_leader_upload_complete())
                else:
                    await self._decrement_assignment("self")
                raise
            except Exception as e:
                print(f"Upload failed (streaming): {e}")
                await self._release_reservation(file_size_gb)
                stage_dir = self.staging_dir / temp_id
                if stage_dir.exists():
                    import shutil as _shutil
                    _shutil.rmtree(stage_dir, ignore_errors=True)
                if is_delegated:
                    asyncio.create_task(self._notify_leader_upload_complete())
                else:
                    await self._decrement_assignment("self")
                raise HTTPException(500, f"Upload failed: {str(e)}")
        
        
        @self.app.post("/api/upload/legacy")
        async def upload_file_legacy(file: UploadFile = FastAPIFile(...)):
            """
            Upload a file with space reservation system
            
            IMPROVED CONCURRENCY:
            - Only reserves space atomically (<1ms lock)
            - Upload proceeds in parallel with other uploads
            - On failure, space is refunded to pool
            - Allows 10-chunk upload + 2-chunk upload simultaneously!
            """
            # Check if we're the leader (with grace period for in-flight requests)
            LEADER_GRACE_PERIOD = 30
            is_current_leader = self.raft and self.raft.is_leader()
            recently_was_leader = (
                not is_current_leader and
                self._leadership_lost_at > 0 and
                (time.time() - self._leadership_lost_at) < LEADER_GRACE_PERIOD
            )
            
            if self.raft and not is_current_leader and not recently_was_leader:
                leader_id = self.raft.get_leader_id()
                if leader_id and leader_id in self.peer_nodes:
                    return await self._proxy_upload_to_leader(file, leader_id)
                else:
                    raise HTTPException(503, "No leader available - cluster electing")
            
            if recently_was_leader:
                print(f"Accepting upload during leadership grace period")
            
            # Peek at file size first (before reading)
            file.file.seek(0, 2)
            file_size = file.file.tell()
            file.file.seek(0)
            file_size_gb = file_size / (1024 ** 3)
            
            # Enforce max upload size
            if file_size > MAX_UPLOAD_SIZE_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large: {file_size / (1024**3):.2f}GB exceeds "
                           f"max {MAX_UPLOAD_SIZE_BYTES / (1024**3):.0f}GB"
                )
            
            # ATOMIC SPACE RESERVATION (fast, <1ms)
            # This is the ONLY serialized part - upload itself is parallel
            space_reserved = await self._reserve_space(file_size_gb)
            
            if not space_reserved:
                self._update_capacity()
                available = self.free_capacity_gb - self.reserved_space_gb
                raise HTTPException(
                    status_code=507,  # Insufficient Storage
                    detail=f"Insufficient storage: need {file_size_gb:.2f}GB, "
                           f"available {available:.2f}GB (reserved: {self.reserved_space_gb:.2f}GB)"
                )
            
            # Space reserved! Upload can proceed in parallel with others
            print(f"Upload started: {file.filename} ({file_size:,} bytes)")
            print(f"Space reserved: {file_size_gb:.2f}GB (lock held <1ms)")
            
            db = self.SessionLocal()
            chunk_paths = []
            
            try:
                # BEGIN ATOMIC TRANSACTION
                # All database operations happen in a single transaction
                # Either ALL succeed or NONE (rollback on any failure)
                
                # Get chunk size from settings (default 64MB)
                CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB in bytes
                
                # Calculate number of chunks needed
                total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE  # Ceiling division
                
                print(f" File will be split into {total_chunks} chunk(s) of {CHUNK_SIZE / (1024**2):.0f}MB each")
                print(f" Other uploads can proceed in parallel!")
                
                # Read entire file content for overall checksum
                # Use asyncio.to_thread to prevent blocking the event loop
                content = await asyncio.to_thread(file.file.read)
                
                # Allow event loop to process other requests (like heartbeats)
                await asyncio.sleep(0)
                
                # Calculate full file checksum
                file_hash = hashlib.sha256()
                file_hash.update(content)
                file_checksum = file_hash.hexdigest()
                
                # Create file record (NOT committed yet)
                file_record = FileModel(
                    filename=file.filename,
                    total_size_bytes=file_size,
                    chunk_size_bytes=CHUNK_SIZE,
                    total_chunks=total_chunks,
                    checksum_sha256=file_checksum,
                    uploaded_by="web_user",
                    is_complete=False  # Will be set to True only if ALL chunks succeed
                )
                db.add(file_record)
                await db_flush_with_retry(db)  # Get ID without committing
                
                print(f" Created file record (ID: {file_record.id})")
                
                # Process and store each chunk
                chunk_records = []
                for chunk_index in range(total_chunks):
                    start_byte = chunk_index * CHUNK_SIZE
                    end_byte = min(start_byte + CHUNK_SIZE, file_size)
                    chunk_data = content[start_byte:end_byte]
                    chunk_size = len(chunk_data)
                    
                    # Content hash = chunk identity
                    chunk_checksum = hashlib.sha256(chunk_data).hexdigest()
                    
                    # Canonical Chunk record (insert-if-not-exists, manages refcount)
                    self._ensure_chunk(db, chunk_checksum, chunk_size)
                    
                    # Create FileChunk mapping
                    file_chunk = FileChunk(
                        file_id=file_record.id,
                        chunk_index=chunk_index,
                        chunk_hash=chunk_checksum,
                    )
                    db.add(file_chunk)
                    
                    # Write to content-addressed store
                    chunk_path = self.chunks_dir / f"{chunk_checksum}.dat"
                    try:
                        if not chunk_path.exists():
                            def write_chunk():
                                with open(chunk_path, 'wb') as f:
                                    f.write(chunk_data)
                            
                            await asyncio.to_thread(write_chunk)
                            chunk_paths.append(chunk_path)
                            
                            def verify_chunk():
                                with open(chunk_path, 'rb') as f:
                                    written_data = f.read()
                                return hashlib.sha256(written_data).hexdigest()
                            
                            written_checksum = await asyncio.to_thread(verify_chunk)
                            if written_checksum != chunk_checksum:
                                raise Exception(f"Chunk {chunk_index} verification failed: checksum mismatch after write")
                        else:
                            # Dedup: chunk already on disk
                            chunk_paths.append(chunk_path)
                        
                        await asyncio.sleep(0)
                    except IOError as disk_error:
                        raise Exception(f"Disk write failed for chunk {chunk_index}: {disk_error}")
                    
                    # Track chunk location on this node
                    existing_loc = db.query(ChunkLocation).filter(
                        ChunkLocation.chunk_hash == chunk_checksum,
                        ChunkLocation.node_id == self.node_id
                    ).first()
                    if not existing_loc:
                        location = ChunkLocation(
                            chunk_hash=chunk_checksum,
                            node_id=self.node_id,
                        )
                        db.add(location)
                    
                    chunk_records.append(file_chunk)
                    
                    print(f"Chunk {chunk_index + 1}/{total_chunks}: {chunk_size:,} bytes → {chunk_checksum[:12]}...")
                
                # ALL chunks written successfully to disk!
                # Now mark file as complete
                file_record.is_complete = True
                file_record.is_replicated = False  # Will be set after replication
                
                # COMMIT ATOMIC TRANSACTION (with retry for concurrent access)
                # This is the ONLY commit - either all succeeds or all fails
                await db_commit_with_retry(db)
                
                # COMMIT SPACE RESERVATION (move from reserved to used)
                await self._commit_reservation(file_size_gb)
                
                print(f"Upload complete: {file.filename} (ID: {file_record.id}, {total_chunks} chunks)")
                print(f" Transaction committed atomically")
                print(f" Space reservation committed: {file_size_gb:.2f}GB")
                
                # REPLICATION: Replicate to peers if available
                if len(self.peer_nodes) > 0:
                    print(f"Starting replication of {total_chunks} chunks...")
                    asyncio.create_task(self._replicate_file_chunks(file_record.id))
                
                return {
                    "message": "File uploaded successfully",
                    "file_id": file_record.id,
                    "filename": file.filename,
                    "size_bytes": file_size,
                    "total_chunks": total_chunks,
                    "chunk_size_bytes": CHUNK_SIZE,
                    "checksum": file_checksum,
                    "chunks": [
                        {
                            "chunk_index": c.chunk_index,
                            "size_bytes": c.chunk.size_bytes if c.chunk else 0,
                            "checksum": c.chunk_hash
                        }
                        for c in chunk_records
                    ]
                }
                
            except HTTPException:
                # Release reservation on HTTP errors
                await self._release_reservation(file_size_gb)
                raise
                
            except Exception as e:
                print(f"Upload failed: {e}")
                
                # ROLLBACK: Undo ALL database changes
                db.rollback()
                print(f" Database transaction rolled back")
                
                # RELEASE SPACE RESERVATION (refund to pool)
                await self._release_reservation(file_size_gb)
                print(f" Space reservation released: {file_size_gb:.2f}GB refunded")
                
                # CLEANUP: Delete physical chunk files that were written
                # Only delete if no Chunk record exists (our insert was rolled back,
                # and no prior upload created this chunk)
                for chunk_path in chunk_paths:
                    try:
                        if chunk_path.exists():
                            chunk_hash = chunk_path.stem
                            existing = db.query(Chunk).filter(
                                Chunk.chunk_hash == chunk_hash
                            ).first()
                            if not existing:
                                chunk_path.unlink()
                                print(f" Cleaned up: {chunk_path.name}")
                            else:
                                print(f" Kept (referenced by other file): {chunk_path.name}")
                    except Exception as cleanup_error:
                        print(f" Cleanup failed for {chunk_path.name}: {cleanup_error}")
                
                print(f" Upload completely rolled back (no partial data)")
                
                raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
            finally:
                db.close()

        # Replication endpoint
        @self.app.post("/api/internal/replicate_chunk")
        async def receive_chunk_replica(request: dict):
            """
            Receive chunk replica from leader.
            
            Same architecture as upload — disk first, DB under semaphore:
            Phase 1: Verify signature + decode + checksum (no DB)
            Phase 2: Write chunk to disk (no DB)  
            Phase 3: Batch DB write under _db_write_semaphore (<50ms)
            
            This prevents "database is locked" when multiple files
            replicate concurrently (e.g. 3 files × 8 chunks = 24 hits).
            """
            # ========================================
            # PHASE 1: Verify and extract (no DB)
            # ========================================
            if 'signature' in request and 'public_key' in request:
                verified, message, sender_node_id = self.trust_store.verify_message(request)
                if not verified:
                    print(f"Rejected replication: Invalid signature")
                    raise HTTPException(401, "Invalid signature")
                
                file_id = message['file_id']
                chunk_index = message['chunk_index']
                chunk_data_b64 = message['chunk_data']
                chunk_checksum = message['checksum']
                file_metadata = message.get('file_metadata')
                sender_name = message.get('node_name', 'unknown')
                print(f"Verified replication from {sender_name} ({sender_node_id[:8]}...)")
                
                # IMPLICIT HEARTBEAT: Receiving data from the leader proves it's alive.
                # In Raft, AppendEntries (replication) resets the election timer just
                # like a heartbeat. Without this, heavy I/O can starve the heartbeat
                # task while replication streams keep flowing — causing the follower
                # to start unnecessary elections even though the leader is clearly alive.
                if self.raft and self.raft.leader_id == sender_node_id:
                    self.raft.last_heartbeat = time.time()
            else:
                file_id = request['file_id']
                chunk_index = request['chunk_index']
                chunk_data_b64 = request['chunk_data']
                chunk_checksum = request['checksum']
                file_metadata = request.get('file_metadata')
                print(f"Accepting UNSIGNED replication (legacy mode)")
            
            import base64
            chunk_data = base64.b64decode(chunk_data_b64)
            
            import hashlib
            actual_checksum = hashlib.sha256(chunk_data).hexdigest()
            if actual_checksum != chunk_checksum:
                raise HTTPException(400, "Checksum mismatch")
            
            # ========================================
            # PHASE 2: Write to content-addressed store (no DB lock)
            # ========================================
            chunk_path = self.chunks_dir / f"{chunk_checksum}.dat"
            if not chunk_path.exists():
                await asyncio.to_thread(chunk_path.write_bytes, chunk_data)
            # else: chunk already exists (dedup from another file/upload)
            
            # ========================================
            # PHASE 3: DB write under semaphore (fast)
            # ========================================
            async with self._db_write_semaphore:
                db = self.SessionLocal()
                try:
                    # Ensure file record exists
                    existing_file = db.query(FileModel).filter(FileModel.id == file_id).first()
                    
                    if existing_file and file_metadata and existing_file.checksum_sha256 == "pending_delegation":
                        existing_file.filename = file_metadata['filename']
                        existing_file.total_size_bytes = file_metadata['total_size_bytes']
                        existing_file.chunk_size_bytes = file_metadata['chunk_size_bytes']
                        existing_file.total_chunks = file_metadata['total_chunks']
                        existing_file.checksum_sha256 = file_metadata['checksum_sha256']
                        existing_file.uploaded_by = file_metadata.get('uploaded_by', 'delegated')
                        await db_flush_with_retry(db)
                        print(f"Updated delegation placeholder: {file_metadata['filename']} (ID: {file_id})")
                    elif not existing_file and file_metadata:
                        file_record = FileModel(
                            id=file_id,
                            filename=file_metadata['filename'],
                            total_size_bytes=file_metadata['total_size_bytes'],
                            chunk_size_bytes=file_metadata['chunk_size_bytes'],
                            total_chunks=file_metadata['total_chunks'],
                            checksum_sha256=file_metadata['checksum_sha256'],
                            uploaded_by=file_metadata.get('uploaded_by', 'replicated'),
                            is_complete=False
                        )
                        db.add(file_record)
                        await db_flush_with_retry(db)
                        print(f"Created file record: {file_metadata['filename']} (ID: {file_id})")
                    elif not existing_file and not file_metadata:
                        db.close()
                        print(f"Chunk {chunk_index} for file {file_id}: no file record yet, requesting retry with metadata")
                        raise HTTPException(409, f"File record {file_id} not found - retry with file_metadata")
                    
                    # Canonical Chunk record (insert-if-not-exists)
                    # increment_ref=False because replication doesn't create new file references
                    self._ensure_chunk(db, chunk_checksum, len(chunk_data), increment_ref=False)
                    
                    # Create FileChunk mapping (if not already present)
                    existing_fc = db.query(FileChunk).filter(
                        FileChunk.file_id == file_id,
                        FileChunk.chunk_index == chunk_index
                    ).first()
                    
                    if not existing_fc:
                        file_chunk = FileChunk(
                            file_id=file_id,
                            chunk_index=chunk_index,
                            chunk_hash=chunk_checksum,
                        )
                        db.add(file_chunk)
                    
                    # Track chunk location on this node (dedup-safe)
                    existing_loc = db.query(ChunkLocation).filter(
                        ChunkLocation.chunk_hash == chunk_checksum,
                        ChunkLocation.node_id == self.node_id
                    ).first()
                    
                    if not existing_loc:
                        location = ChunkLocation(
                            chunk_hash=chunk_checksum,
                            node_id=self.node_id,
                        )
                        db.add(location)
                    
                    # Check if all chunks received
                    file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                    if file_record:
                        chunks_received = db.query(FileChunk).filter(FileChunk.file_id == file_id).count()
                        if chunks_received == file_record.total_chunks:
                            file_record.is_complete = True
                            file_record.is_replicated = True
                            print(f"File complete: {file_record.filename} ({file_record.total_chunks} chunks)")
                    
                    await db_commit_with_retry(db)
                except HTTPException:
                    raise
                except Exception as e:
                    db.rollback()
                    print(f"Replication failed: {e}")
                    raise HTTPException(500, f"Replication failed: {str(e)}")
                finally:
                    db.close()
            
            print(f"Received replica: {chunk_checksum[:12]}... (chunk {chunk_index} of file {file_id}, {len(chunk_data)} bytes)")
            
            return {
                "status": "ack",
                "file_id": file_id, 
                "chunk_index": chunk_index,
                "checksum": chunk_checksum,
                "received_at": time.time()
            }

        # Download endpoint
        @self.app.get("/api/download/{file_id}")
        async def download_file(file_id: int):
            """
            Download a file - streams chunks directly from disk.
            
            Content-addressed: looks up FileChunk mappings to find
            chunk hashes, then reads from chunks/{hash}.dat in order.
            Memory usage: ~64MB (one chunk at a time).
            """
            from starlette.responses import StreamingResponse
            
            db = self.SessionLocal()
            try:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file_record:
                    raise HTTPException(status_code=404, detail="File not found")
                
                if not file_record.is_complete:
                    raise HTTPException(status_code=409, detail="File upload not complete")
                
                filename = file_record.filename
                total_size = file_record.total_size_bytes
                total_chunks = file_record.total_chunks
                
                print(f"Download: {filename} (ID: {file_id}, {total_chunks} chunks)")
                
                # Check for legacy complete file (old-style single file uploads)
                complete_file_path = self.config.storage_path / f"file_{file_id}.dat"
                if complete_file_path.exists():
                    print(f" Using legacy complete file: {complete_file_path}")
                    db.close()
                    return FileResponse(
                        path=complete_file_path,
                        filename=filename,
                        media_type="application/octet-stream"
                    )
                
                # Look up content-addressed chunk hashes in order
                file_chunks = db.query(FileChunk).filter(
                    FileChunk.file_id == file_id
                ).order_by(FileChunk.chunk_index).all()
                
                chunk_paths = []
                for fc in file_chunks:
                    chunk_path = self.chunks_dir / f"{fc.chunk_hash}.dat"
                    if not chunk_path.exists():
                        # Fallback: check legacy path
                        legacy_path = self.config.storage_path / f"file_{file_id}_chunk_{fc.chunk_index}.dat"
                        if legacy_path.exists():
                            chunk_path = legacy_path
                        else:
                            raise HTTPException(
                                status_code=500,
                                detail=f"Missing chunk {fc.chunk_index} ({fc.chunk_hash[:12]}...) for file {file_id}"
                            )
                    chunk_paths.append(chunk_path)
                
            finally:
                db.close()
            
            # Stream chunks directly from disk — no temp file, constant memory
            async def chunk_streamer():
                for chunk_path in chunk_paths:
                    data = await asyncio.to_thread(chunk_path.read_bytes)
                    yield data
                    await asyncio.sleep(0)  # Yield to event loop between chunks
            
            return StreamingResponse(
                chunk_streamer(),
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "Content-Length": str(total_size),
                }
            )
        
        # Delete file endpoint
        @self.app.delete("/api/files/{file_id}")
        async def delete_file(file_id: int):
            """Delete a file. Only removes chunk data if no other file references the chunk."""
            db = self.SessionLocal()
            try:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file_record:
                    raise HTTPException(status_code=404, detail="File not found")
                
                filename = file_record.filename
                
                # Get chunk hashes before deleting the file record
                file_chunks = db.query(FileChunk).filter(FileChunk.file_id == file_id).all()
                chunk_hashes = [fc.chunk_hash for fc in file_chunks]
                
                # Delete file record (cascades to file_chunks)
                db.delete(file_record)
                await db_flush_with_retry(db)
                
                # Decrement refcount on each chunk — GC if unreferenced
                gc_count = 0
                for ch in chunk_hashes:
                    if self._decrement_chunk_ref(db, ch):
                        gc_count += 1
                
                await db_commit_with_retry(db)
                
                print(f"Deleted: {filename} (ID: {file_id}, {len(chunk_hashes)} mappings, {gc_count} chunks garbage collected)")
                
                # Clean up replicas on peer nodes (fire-and-forget)
                if self.peer_nodes:
                    asyncio.create_task(
                        self._delete_remote_replicas(file_id, len(chunk_hashes), chunk_hashes)
                    )
                
                return {"message": "File deleted successfully"}
            finally:
                db.close()
        
        @self.app.post("/api/internal/delete_chunks")
        async def receive_delete_request(request: dict):
            """Receive chunk deletion request from leader (content-addressed)"""
            db = self.SessionLocal()
            try:
                if 'signature' in request and 'public_key' in request:
                    verified, message, sender_node_id = self.trust_store.verify_message(request)
                    if not verified:
                        raise HTTPException(401, "Invalid signature")
                    file_id = message['file_id']
                    chunk_hashes = message.get('chunk_hashes', [])
                else:
                    file_id = request.get('file_id')
                    chunk_hashes = request.get('chunk_hashes', [])
                    if file_id is None:
                        raise HTTPException(400, "Missing file_id")
                
                # Get chunk hashes BEFORE deleting file (cascade removes FileChunks)
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if file_record:
                    file_chunks = db.query(FileChunk).filter(FileChunk.file_id == file_id).all()
                    file_chunk_hashes = [fc.chunk_hash for fc in file_chunks]
                    
                    # Delete file record (cascades to file_chunks)
                    db.delete(file_record)
                    await db_flush_with_retry(db)
                    
                    # Decrement refcount on each chunk — GC if unreferenced
                    deleted_count = 0
                    for ch in file_chunk_hashes:
                        if self._decrement_chunk_ref(db, ch):
                            deleted_count += 1
                    
                    await db_commit_with_retry(db)
                else:
                    deleted_count = 0
                
                print(f"Deleted remote replicas: file {file_id} ({deleted_count} chunks removed)")
                return {"status": "ok", "deleted_chunks": deleted_count}
            
            except HTTPException:
                raise
            except Exception as e:
                db.rollback()
                print(f"Remote delete failed: {e}")
                raise HTTPException(500, f"Delete failed: {str(e)}")
            finally:
                db.close()
        
        @self.app.get("/api/cluster/info")
        async def get_cluster_info():
            return {
                "node_id": self.config.node_id,
                "node_name": self.config.node_name,
                "cluster_id": self.config.cluster_id,
                "is_leader": self.is_leader,
                "cluster_mode": self.config.cluster_mode,
                "ip_address": self.local_ip,
                "port": self.config.port,
                "storage_limit_gb": self.config.storage_limit_gb
            }
        
        @self.app.get("/api/health")
        async def health_check():
            # Update capacity before responding
            self._update_capacity()
            
            return {
                "status": "healthy",
                "node_id": self.node_id,  # Use cryptographic node ID
                "node_name": self.config.node_name,
                "cluster_id": self.config.cluster_id,
                "total_capacity_gb": round(self.total_capacity_gb, 2),
                "free_capacity_gb": round(self.free_capacity_gb, 2),
                "used_capacity_gb": round(self.used_capacity_gb, 2),
                "upload_slots_available": self._upload_semaphore._value,
                "upload_slots_total": 3,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

        @self.app.get("/api/inventory")
        async def get_inventory():
            """
            Lightweight file inventory for leader reconciliation.
            
            Content-addressed: reports chunk hashes physically present on disk,
            plus file metadata from database. The new leader can rebuild the
            global file→chunk mapping from this.
            """
            db = self.SessionLocal()
            try:
                files = db.query(FileModel).all()
                inventory = []
                
                for f in files:
                    file_chunks = db.query(FileChunk).filter(
                        FileChunk.file_id == f.id
                    ).order_by(FileChunk.chunk_index).all()
                    
                    # Verify chunks actually exist on disk (content-addressed)
                    chunks_on_disk = []
                    for fc in file_chunks:
                        chunk_path = self.chunks_dir / f"{fc.chunk_hash}.dat"
                        if not chunk_path.exists():
                            # Check legacy path
                            chunk_path = self.config.storage_path / f"file_{f.id}_chunk_{fc.chunk_index}.dat"
                        if chunk_path.exists():
                            chunk_record = db.query(Chunk).filter(Chunk.chunk_hash == fc.chunk_hash).first()
                            chunks_on_disk.append({
                                "chunk_index": fc.chunk_index,
                                "size_bytes": chunk_record.size_bytes if chunk_record else chunk_path.stat().st_size,
                                "chunk_hash": fc.chunk_hash,
                            })
                    
                    inventory.append({
                        "file_id": f.id,
                        "filename": f.filename,
                        "total_size_bytes": f.total_size_bytes,
                        "chunk_size_bytes": f.chunk_size_bytes,
                        "total_chunks": f.total_chunks,
                        "checksum_sha256": f.checksum_sha256,
                        "is_complete": f.is_complete,
                        "uploaded_by": f.uploaded_by,
                        "chunks_on_disk": chunks_on_disk,
                    })
                
                # Scan content-addressed store for all chunk hashes on disk
                # This is the REAL source of truth — physical data present
                all_chunk_hashes = []
                if self.chunks_dir.exists():
                    for dat_file in self.chunks_dir.glob("*.dat"):
                        chunk_hash = dat_file.stem  # filename without .dat
                        if len(chunk_hash) == 64:  # valid SHA-256 hex
                            all_chunk_hashes.append(chunk_hash)
                
                # Find chunks on disk not referenced by any file (orphans)
                referenced_hashes = set()
                for f_info in inventory:
                    for c in f_info["chunks_on_disk"]:
                        referenced_hashes.add(c["chunk_hash"])
                
                orphaned_chunks = [h for h in all_chunk_hashes if h not in referenced_hashes]
                
                return {
                    "node_id": self.node_id,
                    "node_name": self.config.node_name,
                    "files": inventory,
                    "all_chunk_hashes": all_chunk_hashes,
                    "orphaned_chunks": orphaned_chunks,
                }
            finally:
                db.close()

        @self.app.get("/api/cluster/stats")
        async def get_stats():
            db = self.SessionLocal()
            try:
                self._update_capacity()
                
                # Count nodes (self + peers)
                total_nodes = 1 + len(self.peer_nodes)
                
                # Count ONLINE nodes (check health status)
                online_nodes = 1  # Self is always online
                for node_id in self.peer_nodes.keys():
                    if self.health_monitor:
                        peer_status = self.health_monitor.get_peer_status(node_id)
                        if peer_status == "online":
                            online_nodes += 1
                    else:
                        # No health monitor, assume all discovered peers are online
                        online_nodes += 1
                
                # DEBUG: Log cluster status
                print(f"[DEBUG] Cluster stats: {online_nodes}/{total_nodes} nodes online")
                
                # Calculate RAW capacity (sum of all nodes)
                raw_capacity = self.total_capacity_gb
                for n in self.peer_nodes.values():
                    raw_capacity += n["storage_gb"]
                
                # Get replication factor based on cluster size
                if total_nodes == 1:
                    replication_factor = 1  # No replication with single node
                elif total_nodes <= 4:
                    replication_factor = 2
                else:
                    replication_factor = 3
                
                # Calculate USABLE capacity (accounting for replication)
                usable_capacity = raw_capacity / replication_factor
                
                # Count unique data (SUM of all file sizes, not replicas)
                unique_data_result = db.execute(
                    text("SELECT COALESCE(SUM(total_size_bytes), 0) FROM files")
                ).fetchone()
                unique_data_bytes = unique_data_result[0] if unique_data_result else 0
                unique_data_gb = round(unique_data_bytes / (1024 ** 3), 2)
                
                # Count total chunk locations across cluster
                total_replicas = db.execute(
                    text("SELECT COUNT(*) FROM chunk_locations")
                ).fetchone()[0]
                
                # Count chunks
                total_chunks = db.execute(
                    text("SELECT COUNT(*) FROM file_chunks")
                ).fetchone()[0]
                
                # Files count
                file_count = db.query(FileModel).count()
                
                # Calculate actual storage used (unique chunks * sizes on all nodes)
                actual_storage_used = db.execute(
                    text("""
                        SELECT COALESCE(SUM(c.size_bytes), 0)
                        FROM chunk_locations cl
                        JOIN chunks c ON c.chunk_hash = cl.chunk_hash
                    """)
                ).fetchone()[0]
                actual_storage_used_gb = round(actual_storage_used / (1024 ** 3), 2)
                
                # Free space calculation
                # USABLE free = (raw capacity - actual storage used) / replication_factor
                free_capacity = (raw_capacity - actual_storage_used_gb) / replication_factor
                
                return {
                    "total_nodes": total_nodes,
                    "online_nodes": online_nodes,
                    "total_files": file_count,
                    "total_chunks": total_chunks,
                    "total_replicas": total_replicas,
                    "avg_replicas_per_chunk": round(total_replicas / total_chunks, 1) if total_chunks > 0 else 0,
                    
                    # Capacity metrics
                    "raw_capacity_gb": round(raw_capacity, 2),
                    "total_capacity_gb": round(usable_capacity, 2),  # Usable capacity
                    "unique_data_gb": unique_data_gb,  # Actual unique data
                    "actual_storage_used_gb": actual_storage_used_gb,  # Including replicas
                    "free_capacity_gb": round(max(0, free_capacity), 2),
                    "utilization_percent": round((unique_data_gb / usable_capacity * 100), 1) if usable_capacity > 0 else 0,
                    
                    # Replication info
                    "replication_factor": replication_factor,
                    "storage_efficiency_percent": round((usable_capacity / raw_capacity * 100), 1) if raw_capacity > 0 else 0,
                    "replication_complete": total_replicas >= (total_chunks * replication_factor) if replication_factor > 1 else True
                }
            finally:
                db.close()

        @self.app.get("/api/nodes")
        async def get_nodes():
            """Get all nodes in cluster (self + discovered peers)"""
            self._update_capacity()
            now = datetime.now(timezone.utc)
            
            nodes = []
            
            # Add self (always online)
            is_leader = self.raft.is_leader() if self.raft else self.is_leader
            raft_state = self.raft.get_state() if self.raft else "unknown"
            
            nodes.append({
                "id": 1,
                "name": self.config.node_name,
                "ip_address": self.local_ip,
                "port": self.config.port,
                "total_capacity_gb": round(self.total_capacity_gb, 2),
                "free_capacity_gb": round(self.free_capacity_gb, 2),
                "cpu_score": 1.0,
                "priority_score": 1.0,
                "status": "online",
                "is_brain": False,
                "is_leader": is_leader,
                "raft_state": raft_state,
                "last_heartbeat": now.isoformat(),
                "last_heartbeat_seconds_ago": 0
            })
            
            # Add discovered peers with health status
            leader_id = self.raft.get_leader_id() if self.raft else None
            for idx, (node_id, node_info) in enumerate(self.peer_nodes.items(), start=2):
                peer_is_leader = (node_id == leader_id) if leader_id else False
                
                # Get health status from monitor
                peer_status = "online"
                if self.health_monitor:
                    peer_status = self.health_monitor.get_peer_status(node_id)
                
                # Calculate time since last seen
                last_seen_ago = 0
                if self.health_monitor and node_id in self.health_monitor.peer_last_seen:
                    last_seen_ago = int(time.time() - self.health_monitor.peer_last_seen[node_id])
                
                # Get actual free capacity from peer (use cached value or total as fallback)
                peer_free_gb = node_info.get('free_capacity_gb', node_info['storage_gb'])
                peer_total_gb = node_info['storage_gb']
                
                # DEBUG: Log status determination
                print(f"[DEBUG] Node {node_info['node_name']}: status={peer_status}, last_seen={last_seen_ago}s ago, free={peer_free_gb:.1f}GB")
                
                nodes.append({
                    "id": idx,
                    "name": node_info['node_name'],
                    "ip_address": node_info['ip_address'],
                    "port": node_info['port'],
                    "total_capacity_gb": peer_total_gb,
                    "free_capacity_gb": peer_free_gb,  # Use actual free space
                    "cpu_score": 1.0,
                    "priority_score": 1.0,
                    "status": peer_status,  # Use health monitor status
                    "is_brain": False,
                    "is_leader": peer_is_leader,
                    "raft_state": "follower" if not peer_is_leader else "leader",
                    "last_heartbeat": node_info['discovered_at'],
                    "last_heartbeat_seconds_ago": last_seen_ago,  # Show actual time
                    "latency_ms": node_info.get('latency_ms', None)  # Network latency
                })
            
            return nodes
        
        @self.app.get("/api/files")
        async def get_files():
            db = self.SessionLocal()
            try:
                files = db.query(FileModel).order_by(FileModel.created_at.desc()).all()
                
                result = []
                for f in files:
                    # Count unique nodes that have chunks of this file
                    file_chunks = db.query(FileChunk).filter(FileChunk.file_id == f.id).all()
                    chunk_hashes = [fc.chunk_hash for fc in file_chunks]
                    
                    node_ids = set()
                    if chunk_hashes:
                        locations = db.query(ChunkLocation).filter(
                            ChunkLocation.chunk_hash.in_(chunk_hashes)
                        ).all()
                        for loc in locations:
                            node_ids.add(loc.node_id)
                    
                    replica_count = len(node_ids)
                    
                    result.append({
                        "id": f.id,
                        "filename": f.filename,
                        "total_size_bytes": f.total_size_bytes,
                        "size_mb": round(f.total_size_bytes / (1024**2), 2),
                        "total_size_mb": round(f.total_size_bytes / (1024**2), 2),
                        "total_chunks": f.total_chunks,
                        "is_complete": f.is_complete,
                        "is_replicated": replica_count > 1,
                        "replica_count": replica_count,
                        "created_at": f.created_at.isoformat() if f.created_at else None,
                        "uploaded_by": f.uploaded_by
                    })
                
                return result
            finally:
                db.close()
        
        @self.app.get("/api/files/{file_id}")
        async def get_file_details(file_id: int):
            """Get single file details with chunk locations"""
            db = self.SessionLocal()
            try:
                file = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file:
                    raise HTTPException(status_code=404, detail="File not found")
                
                file_chunks = db.query(FileChunk).filter(
                    FileChunk.file_id == file_id
                ).order_by(FileChunk.chunk_index).all()
                
                chunk_info = []
                all_node_ids = set()
                
                for fc in file_chunks:
                    # Find all nodes that have this chunk
                    locations = db.query(ChunkLocation).filter(
                        ChunkLocation.chunk_hash == fc.chunk_hash
                    ).all()
                    
                    location_info = []
                    for loc in locations:
                        node_name = "unknown"
                        if loc.node_id == self.node_id:
                            node_name = self.config.node_name
                        else:
                            for peer_id, peer_info in self.peer_nodes.items():
                                if peer_id == loc.node_id:
                                    node_name = peer_info.get('node_name', 'peer_node')
                                    break
                        
                        location_info.append({
                            "node_id": loc.node_id[:12] + "..." if len(loc.node_id) > 12 else loc.node_id,
                            "node_name": node_name,
                        })
                        all_node_ids.add(loc.node_id)
                    
                    # Get size from canonical Chunk record
                    chunk_record = db.query(Chunk).filter(Chunk.chunk_hash == fc.chunk_hash).first()
                    chunk_size = chunk_record.size_bytes if chunk_record else 0
                    
                    chunk_info.append({
                        "chunk_index": fc.chunk_index,
                        "size_bytes": chunk_size,
                        "chunk_hash": fc.chunk_hash[:16] + "...",
                        "locations": location_info
                    })
                
                return {
                    "id": file.id,
                    "filename": file.filename,
                    "total_size_bytes": file.total_size_bytes,
                    "total_chunks": file.total_chunks,
                    "checksum_sha256": file.checksum_sha256,
                    "is_complete": file.is_complete,
                    "created_at": file.created_at.isoformat() if file.created_at else None,
                    "uploaded_by": file.uploaded_by,
                    "replica_count": len(all_node_ids),
                    "chunks": chunk_info
                }
            finally:
                db.close()
        
        @self.app.get("/api/settings")
        async def get_settings():
            return {
                "chunk_size_mb": 64,
                "min_replicas": 2,
                "max_replicas": 3,
                "replication_strategy": "priority",
                "redundancy_mode": "standard",
                "verify_on_upload": True,
                "parallel_downloads": 4
            }
        
        @self.app.get("/api/jobs/status")
        async def get_jobs_status():
            return {
                "pending": 0,
                "in_progress": 0,
                "completed": 0,
                "failed": 0
            }
        
        @self.app.get("/api/security/stats")
        async def get_security_stats():
            """Security metrics for monitoring"""
            return {
                "identity": {
                    "node_id": self.node_id,
                    "node_name": self.config.node_name,
                    "public_key": self.identity.get_public_key_base64()[:32] + "...",
                    "keys_path": str(self.identity.keys_path)
                },
                "trust": {
                    "trusted_peers": len(self.trust_store.trusted_peers),
                    "peers": [
                        {
                            "node_id": node_id[:8] + "...",
                            "name": info['node_name'],
                            "first_seen": info['first_seen'],
                            "last_seen": info['last_seen']
                        }
                        for node_id, info in self.trust_store.trusted_peers.items()
                    ]
                }
            }
    
    async def _delete_remote_replicas(self, file_id: int, chunk_count: int, chunk_hashes: list = None):
        """
        Send delete requests to all peer nodes to remove replicated chunks.
        Fire-and-forget with best-effort delivery.
        """
        if not self.peer_nodes:
            return
        
        print(f"Cleaning up remote replicas for file {file_id}...")
        
        for node_id, peer_info in self.peer_nodes.items():
            try:
                target_url = f"http://{peer_info['ip_address']}:{peer_info['port']}/api/internal/delete_chunks"
                
                delete_message = {
                    'file_id': file_id,
                    'chunk_hashes': chunk_hashes or [],
                    'node_name': self.config.node_name,
                    'timestamp': time.time()
                }
                
                if self.identity:
                    signed_payload = self.identity.sign_json(delete_message)
                else:
                    signed_payload = delete_message
                
                session = await self._get_http_session()
                async with session.post(
                    target_url,
                    json=signed_payload,
                    timeout=aiohttp.ClientTimeout(total=5)
                ) as response:
                    if response.status == 200:
                        print(f"Cleaned replicas on {peer_info['node_name']}")
                    else:
                        print(f"Cleanup failed on {peer_info['node_name']}: HTTP {response.status}")
            except Exception as e:
                print(f"Cleanup failed on {peer_info['node_name']}: {e}")
    
    async def start(self):
        """Start the node"""
        print("\n" + "="*60)
        print(f"Starting TossIt Node: {self.config.node_name}")
        print("="*60)
        print(f"Cluster ID:    {self.config.cluster_id}")
        print(f"Node ID:       {self.config.node_id}")
        print(f"Storage:       {self.config.storage_limit_gb:.1f} GB allocated")
        print(f"Port:          {self.config.port}")
        print("="*60)
        print()
        
        # Verify chunk integrity before accepting any traffic
        await self._verify_chunks_on_boot()
        
        # Start Raft for leader election
        self.raft = ClusterRaft(
            node_id=self.node_id,  # Use cryptographic node ID
            node_name=self.config.node_name,
            port=self.config.port,  # UDP heartbeat will use port+1
            data_dir=str(DB_DIR),   # Persist term/voted_for alongside DB
            identity=self.identity,  # Pass identity for signing messages
            trust_store=self.trust_store,  # Pass trust store for verification
            on_become_leader=self._on_became_leader,
            on_lose_leadership=self._on_lost_leadership,
        )
        await self.raft.start()
        
        # Start health monitor
        self.health_monitor = NodeHealthMonitor(
            timeout_seconds=60.0,  # Mark offline after 60 seconds (generous for heavy I/O)
            check_interval=10.0,
            on_node_offline=self._on_node_went_offline
        )
        await self.health_monitor.start()
        
        # Start periodic peer refresh
        self.peer_refresh_task = asyncio.create_task(self._periodic_peer_refresh())
        
        # Start discovery
        self.discovery = ClusterDiscovery(
            cluster_id=self.config.cluster_id,
            node_id=self.config.node_id,
            node_name=self.config.node_name,
            port=self.config.port,
            storage_gb=self.config.storage_limit_gb,
            on_node_discovered=self._on_node_discovered,
            on_node_lost=self._on_node_lost
        )
        
        await self.discovery.start()
        
        # Initialize registry client if URL provided
        registry_url = os.getenv('TOSSIT_REGISTRY_URL')
        if registry_url:
            self.registry_client = RegistryClient(
                registry_url=registry_url,
                cluster_id=self.config.cluster_id,
                node_name=self.config.node_name,
                port=self.config.port
            )
            print(f"Registry configured: {registry_url}")
            
            # Test connection
            registry_ok = await self.registry_client.test_connection()
            if registry_ok:
                print("✓ Registry service reachable")
            else:
                print("Warning: Registry service not reachable (will retry)")
            
            # Start periodic stats update task
            asyncio.create_task(self._update_registry_stats_loop())
        else:
            print("No registry configured (set TOSSIT_REGISTRY_URL to enable)")
        
        # Start web server
        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.config.port,
            log_level="info"
        )
        server = uvicorn.Server(config)
        
        # Run both concurrently
        try:
            await server.serve()
        finally:
            # Cleanup
            if self.peer_refresh_task:
                self.peer_refresh_task.cancel()
                try:
                    await self.peer_refresh_task
                except asyncio.CancelledError:
                    pass
            
            if self.discovery:
                await self.discovery.stop()
            if self.raft:
                await self.raft.stop()
            if self.health_monitor:
                await self.health_monitor.stop()
            
            # Close persistent HTTP session
            if self._http_session and not self._http_session.closed:
                await self._http_session.close()
    
    def _calculate_total_capacity(self) -> float:
        """Calculate total cluster capacity in GB"""
        total = self.config.storage_limit_gb
        
        # Add capacity from all peer nodes
        for node_info in self.peer_nodes.values():
            total += node_info.get('storage_gb', 0)
        
        return round(total, 2)
    
    def _calculate_used_capacity(self) -> float:
        """
        Calculate used capacity in GB
        
        Counts all chunk replicas on this node. No longer stores complete files,
        so storage = sum of all chunk sizes.
        """
        db = self.SessionLocal()
        try:
            # Count ALL unique chunks on THIS node
            result = db.execute(
                text("""
                    SELECT COALESCE(SUM(c.size_bytes), 0)
                    FROM chunk_locations cl
                    JOIN chunks c ON c.chunk_hash = cl.chunk_hash
                    WHERE cl.node_id = :nid
                """),
                {"nid": self.node_id}
            ).fetchone()
            
            if result:
                used_bytes = result[0]
                return round(used_bytes / (1024 ** 3), 2)
            
            return 0.0
        except Exception as e:
            print(f"Error calculating used capacity: {e}")
            return 0.0
        finally:
            db.close()
    
    async def _update_registry_stats_loop(self):
        """
        Periodically update registry with latest cluster stats
        Only runs if this node is the leader
        """
        while True:
            try:
                await asyncio.sleep(30)  # Update every 30 seconds
                
                if self.registry_client and self.is_leader:
                    # Calculate current stats
                    node_count = len(self.peer_nodes) + 1
                    total_capacity_gb = self._calculate_total_capacity()
                    used_capacity_gb = self._calculate_used_capacity()
                    
                    # Update registry client
                    self.registry_client.update_stats(
                        node_count=node_count,
                        total_capacity_gb=total_capacity_gb,
                        used_capacity_gb=used_capacity_gb,
                        local_ip=self.local_ip
                    )
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error updating registry stats: {e}")
                await asyncio.sleep(30)


async def main():
    """Main entry point"""
    config = NodeConfig.load()
    
    if not config.exists():
        config = interactive_setup()
    else:
        print(f"Using existing configuration")
        print(f"Node: {config.node_name}")
        print(f"Cluster: {config.cluster_id}")
        print(f"Storage: {config.storage_limit_gb:.1f} GB")
    
    node = TossItNode(config)
    await node.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n TossIt node stopped")
        sys.exit(0)