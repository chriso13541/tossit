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
from fastapi.responses import StreamingResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
import uvicorn


# Maximum upload size (10 GB)
MAX_UPLOAD_SIZE_BYTES = 10 * 1024 * 1024 * 1024

# ---------------------------------------------------------------------------
# DATA ROOT — single source of truth for all persistent state.
#
# In bare-metal / dev:   defaults to ~/.tossit  (same as before)
# In Docker:             set TOSSIT_DATA_DIR=/data  (mapped volume)
#
# Everything under DATA_ROOT:
#   database/   — SQLite db + Raft state
#   storage/    — chunks, staging, upload_temp
#   keys/       — per-node Ed25519 keypairs
#   trust_store.json  — TOFU peer trust records
#   node_config.yaml  — persisted node/cluster config
# ---------------------------------------------------------------------------
DATA_ROOT       = Path(os.environ.get('TOSSIT_DATA_DIR', Path.home() / '.tossit')).resolve()
CONFIG_DIR      = DATA_ROOT
CONFIG_FILE     = DATA_ROOT / "node_config.yaml"
STORAGE_DIR     = DATA_ROOT / "storage"
DB_DIR          = DATA_ROOT / "database"
KEYS_DIR        = DATA_ROOT / "keys"
TRUST_STORE_FILE = DATA_ROOT / "trust_store.json"

# Ensure base directories exist on import so every subsystem can rely on them.
for _d in (DATA_ROOT, STORAGE_DIR, DB_DIR, KEYS_DIR):
    _d.mkdir(parents=True, exist_ok=True)


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
        """Generate unique node ID (used as placeholder; overwritten by crypto ID at runtime)"""
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

        print(f"Configuration saved to {CONFIG_FILE}")

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

            print(f"Configuration loaded from {CONFIG_FILE}")
            return config

        except Exception as e:
            print(f"Error loading config: {e}")
            return config

    @staticmethod
    def from_env() -> Optional['NodeConfig']:
        """
        Build NodeConfig purely from environment variables.

        Required env vars:
            TOSSIT_NODE_NAME   — human-readable name for this node
            TOSSIT_CLUSTER_ID  — 8-char hex cluster identifier

        Optional env vars:
            TOSSIT_PORT              (default 8000)
            TOSSIT_STORAGE_LIMIT_GB  (default 50)
            TOSSIT_FIRST_NODE        (1 / true / yes  →  True)

        Returns None when the required vars are absent so callers can fall
        through to the YAML config or the interactive setup wizard.
        """
        node_name  = os.environ.get('TOSSIT_NODE_NAME', '').strip()
        cluster_id = os.environ.get('TOSSIT_CLUSTER_ID', '').strip()

        if not node_name or not cluster_id:
            return None

        config = NodeConfig()
        config.node_name        = node_name
        config.cluster_id       = cluster_id
        config.node_id          = NodeConfig.generate_node_id()   # placeholder; overwritten by crypto ID
        config.port             = int(os.environ.get('TOSSIT_PORT', '8000'))
        config.storage_limit_gb = float(os.environ.get('TOSSIT_STORAGE_LIMIT_GB', '50'))
        config.is_first_node    = os.environ.get('TOSSIT_FIRST_NODE', '').lower() in ('1', 'true', 'yes')
        config.cluster_mode     = 'private'
        config.center_enabled   = False

        print(f"Configuration loaded from environment variables")
        print(f"  NODE_NAME  = {config.node_name}")
        print(f"  CLUSTER_ID = {config.cluster_id}")
        print(f"  PORT       = {config.port}")
        print(f"  STORAGE    = {config.storage_limit_gb} GB")
        print(f"  FIRST_NODE = {config.is_first_node}")

        return config

    def exists(self) -> bool:
        """Check if configuration exists"""
        return CONFIG_FILE.exists() and self.node_id is not None


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
    Scans the content-addressed chunks/ directory.
    Returns: used_gb
    """
    total_bytes = 0

    if storage_path.exists():
        chunks_dir = storage_path / "chunks"
        if chunks_dir.exists():
            for chunk_file in chunks_dir.glob("*.dat"):
                try:
                    total_bytes += chunk_file.stat().st_size
                except Exception:
                    pass

    used_gb = total_bytes / (1024 ** 3)
    return used_gb


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

    # Generate placeholder node ID (will be replaced by crypto ID at runtime)
    config.node_id = NodeConfig.generate_node_id()
    print(f"Generated node ID: {config.node_id}")

    # Storage allocation
    print("\n--- Storage Configuration ---")
    total_gb, used_gb, free_gb = get_disk_usage(Path.home())
    print(f"Available disk space: {free_gb:.1f} GB free of {total_gb:.1f} GB total")

    while True:
        storage_input = input(f"\nStorage to allocate (GB) [50]: ").strip()

        if not storage_input:
            config.storage_limit_gb = min(50.0, free_gb * 0.9)
            break

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

    print(f"Allocated: {config.storage_limit_gb:.1f} GB")

    # Cluster setup
    print("\n--- Cluster Setup ---")
    print("1. Create new cluster (I'm the first node)")
    print("2. Join existing cluster (someone else already created it)")

    choice = input("\nChoose [1/2]: ").strip()

    if choice == "1":
        config.is_first_node = True
        config.cluster_id = NodeConfig.generate_cluster_id()
        print(f"\nCreated new cluster: {config.cluster_id}")
        print(f"\nIMPORTANT: Share this cluster ID with others to join:")
        print(f"{config.cluster_id}")
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
            print(f"Will join cluster: {cluster_id}")
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
        import tempfile
        import os
        self.config.storage_path.mkdir(parents=True, exist_ok=True)
        upload_temp_dir = self.config.storage_path / "upload_temp"
        upload_temp_dir.mkdir(exist_ok=True, parents=True)
        os.environ['TMPDIR'] = str(upload_temp_dir)
        tempfile.tempdir = str(upload_temp_dir)
        print(f"Upload temp: {upload_temp_dir} (bypasses tmpfs)")

        # Content-addressed storage directories
        self.chunks_dir = self.config.storage_path / "chunks"
        self.chunks_dir.mkdir(exist_ok=True, parents=True)
        self.staging_dir = self.config.storage_path / "staging"
        self.staging_dir.mkdir(exist_ok=True, parents=True)
        print(f"Content-addressed storage: {self.chunks_dir}")
        print(f"Upload staging: {self.staging_dir}")

        # SECURITY: Initialize cryptographic identity.
        # Keys live under DATA_ROOT/keys/<node_name>/ — fully portable,
        # never scattered into the user's home directory.
        keys_path = KEYS_DIR / config.node_name
        self.identity = NodeIdentity(config.node_name, keys_path)

        # SECURITY: Initialize trust store.
        # Single file at DATA_ROOT/trust_store.json — survives across restarts
        # and is included in whatever volume/backup covers DATA_ROOT.
        self.trust_store = TrustStore(TRUST_STORE_FILE)

        # Use cryptographic node ID (overrides the config.node_id)
        self.node_id = self.identity.get_node_id()

        print(f"Node identity:")
        print(f"  Name:       {config.node_name}")
        print(f"  ID:         {self.node_id}")
        print(f"  Public key: {self.identity.get_public_key_base64()[:32]}...")
        print(f"  Keys path:  {keys_path}")
        print(f"  Trust store: {TRUST_STORE_FILE}")

        # Discovery and Raft setup
        self.discovery: Optional[ClusterDiscovery] = None
        self.raft: Optional[ClusterRaft] = None
        self.peer_nodes: Dict[str, dict] = {}  # node_id -> node_info

        # Health monitoring
        self.health_monitor: Optional[NodeHealthMonitor] = None
        self.peer_refresh_task: Optional[asyncio.Task] = None

        # Registry client (optional)
        self.registry_client: Optional[RegistryClient] = None

        # Space reservation system
        self.reservation_lock = asyncio.Lock()
        self.reserved_space_gb = 0.0

        # Capacity cache
        self._cached_capacity_time = 0.0
        self._capacity_cache_ttl = 2.0

        # Persistent aiohttp session for inter-node communication
        self._http_session: Optional[aiohttp.ClientSession] = None

        # DB write semaphore
        self._db_write_semaphore = asyncio.Semaphore(1)

        # Upload concurrency limiter
        self._upload_semaphore = asyncio.Semaphore(3)

        # UPLOAD LOAD BALANCER STATE
        self._assigned_uploads: Dict[str, int] = {}
        self._assigned_uploads_lock = asyncio.Lock()

        # Replication concurrency limiter
        self._replication_semaphore = asyncio.Semaphore(4)

        # Database setup
        db_path = DB_DIR / f"tossit_{config.node_id}.db"
        self.engine = create_engine(
            f'sqlite:///{db_path}',
            connect_args={
                "check_same_thread": False,
                "timeout": 30,
            },
            echo=False,
            pool_pre_ping=True,
        )

        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragmas(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.execute("PRAGMA wal_autocheckpoint=1000")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()

        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

        self.is_leader = config.is_first_node
        self._leadership_lost_at = 0
        self.local_ip = get_local_ip()

        # Clean up any temp files from interrupted uploads
        self._cleanup_temp_files()

        self._ensure_node_record()

        # Calculate actual disk usage
        self._update_capacity()

        self._setup_routes()
        print(f"Database initialized: {db_path}")

    async def _on_node_discovered(self, node_info: dict):
        """Called when a new node is discovered"""
        node_id = node_info['node_id']
        self.peer_nodes[node_id] = node_info

        print(f"Peer joined: {node_info['node_name']} ({node_info['ip_address']})")

        if self.raft:
            self.raft.update_peers(self.peer_nodes)

        if self.health_monitor:
            self.health_monitor.update_peer(node_id)

        if self.raft and self.raft.is_leader():
            await asyncio.sleep(2)
            print(f"Leader initiating redistribution for new node...")
            await self._redistribute_existing_files()

    async def _on_node_lost(self, node_id: str):
        """Called when a node leaves"""
        if node_id in self.peer_nodes:
            node_info = self.peer_nodes[node_id]
            print(f"Peer left cluster: {node_info['node_name']}")
            del self.peer_nodes[node_id]

            if self.raft:
                self.raft.update_peers(self.peer_nodes)

    async def _on_node_went_offline(self, node_id: str):
        """Called when health monitor detects a node is offline."""
        if self.raft and self.raft.is_peer_alive(node_id, max_age=30.0):
            return  # Heartbeat thread says peer is alive — ignore health monitor

        if node_id in self.peer_nodes:
            node_info = self.peer_nodes[node_id]
            print(f"Node went offline: {node_info['node_name']} (health monitor timeout, confirmed by heartbeat thread)")
            print(f"Raft peers unchanged — Raft handles leader detection independently")

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

                if self.health_monitor and len(self.peer_nodes) > 0:
                    for node_id in list(self.peer_nodes.keys()):
                        peer_info = self.peer_nodes[node_id]
                        peer_url = f"http://{peer_info['ip_address']}:{peer_info['port']}/api/health"

                        try:
                            start_time = time.time()

                            session = await self._get_http_session()
                            async with session.get(
                                peer_url,
                                timeout=aiohttp.ClientTimeout(total=2.0)
                            ) as response:
                                if response.status == 200:
                                    rtt_ms = (time.time() - start_time) * 1000
                                    peer_info['latency_ms'] = round(rtt_ms, 2)

                                    try:
                                        health_data = await response.json()

                                        if 'free_capacity_gb' in health_data:
                                            peer_info['free_capacity_gb'] = health_data['free_capacity_gb']
                                        if 'used_capacity_gb' in health_data:
                                            peer_info['used_capacity_gb'] = health_data['used_capacity_gb']
                                        if 'upload_slots_available' in health_data:
                                            peer_info['upload_slots_available'] = health_data['upload_slots_available']

                                            peer_actual_active = 3 - health_data['upload_slots_available']
                                            our_count = self._assigned_uploads.get(node_id, 0)
                                            if our_count > peer_actual_active:
                                                self._assigned_uploads[node_id] = peer_actual_active

                                    except Exception:
                                        pass

                                    self.health_monitor.update_peer(node_id)
                        except Exception:
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
        print(f"→ Web UI available at: http://{self.local_ip}:{self.config.port}")
        print(f"→ Uploads accepted here")

        if self.registry_client:
            self.registry_client.update_stats(
                cluster_name=f"Cluster {self.config.node_name}",
                node_count=len(self.peer_nodes) + 1,
                total_capacity_gb=self._calculate_total_capacity(),
                used_capacity_gb=self._calculate_used_capacity(),
                local_ip=self.local_ip
            )
            self.registry_client.start_heartbeat()
            print(f"Started registry heartbeat for cluster {self.config.cluster_id}")

        asyncio.create_task(self._reconcile_on_promotion())

    async def _on_lost_leadership(self):
        """Called when this node loses leadership"""
        self.is_leader = False
        self._leadership_lost_at = time.time()
        print("This node is no longer the leader")
        print("→ Uploads should go to the leader node")

        if self.registry_client:
            self.registry_client.stop_heartbeat()
            print("Stopped registry heartbeat")

    def _cleanup_temp_files(self):
        """Remove leftover staging directories from interrupted uploads"""
        import shutil as _shutil

        if self.staging_dir.exists():
            staging_dirs = list(self.staging_dir.iterdir())
            if staging_dirs:
                for d in staging_dirs:
                    try:
                        _shutil.rmtree(d)
                    except Exception:
                        pass
                print(f"Cleaned up {len(staging_dirs)} staging directories")

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
                    status=NodeStatus.ONLINE,
                    last_heartbeat=datetime.now(timezone.utc)
                )
                db.add(node)
                db.commit()
                print(f"Created node record in database (ID: 1)")
            else:
                node.name = self.config.node_name
                node.ip_address = self.local_ip
                node.port = self.config.port
                node.total_capacity_gb = self.config.storage_limit_gb
                node.status = NodeStatus.ONLINE
                node.last_heartbeat = datetime.now(timezone.utc)
                db.commit()
                print(f"Updated existing node record (ID: 1)")
        finally:
            db.close()

    def _update_capacity(self):
        """Update capacity from actual TossIt file storage"""
        used_gb = self._calculate_used_capacity()

        self.total_capacity_gb = self.config.storage_limit_gb
        self.used_capacity_gb = min(used_gb, self.config.storage_limit_gb)
        self.free_capacity_gb = self.config.storage_limit_gb - self.used_capacity_gb

    def _ensure_chunk(self, db, chunk_hash: str, size_bytes: int, increment_ref: bool = True):
        """
        Ensure a Chunk row exists for the given hash. Insert-if-not-exists.
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
        and remove the physical file from disk.
        """
        chunk = db.query(Chunk).filter(Chunk.chunk_hash == chunk_hash).first()
        if not chunk:
            return False

        chunk.refcount -= 1

        if chunk.refcount <= 0:
            chunk_path = self.chunks_dir / f"{chunk_hash}.dat"
            if chunk_path.exists():
                chunk_path.unlink()
            db.delete(chunk)
            return True

        return False

    async def _verify_chunks_on_boot(self):
        """
        Integrity verification — run once on startup.
        Verifies every chunk on disk by rehashing and comparing against filename.
        """
        if not self.chunks_dir.exists():
            return

        print("Verifying chunk integrity on boot...")

        verified = 0
        corrupt = 0
        missing_from_disk = 0

        for dat_file in self.chunks_dir.glob("*.dat"):
            expected_hash = dat_file.stem
            if len(expected_hash) != 64:
                continue

            try:
                actual_hash = await asyncio.to_thread(
                    lambda p=dat_file: hashlib.sha256(p.read_bytes()).hexdigest()
                )

                if actual_hash != expected_hash:
                    corrupt += 1
                    print(f"Corrupt chunk: {expected_hash[:12]}... (actual: {actual_hash[:12]}...)")
                    dat_file.unlink()

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
            print(f"Integrity check: {verified} OK, {corrupt} corrupt (removed), {missing_from_disk} missing from disk (cleaned)")
        else:
            print(f"Integrity check: {verified} chunks verified OK")

    async def _write_chunk_to_disk(
        self,
        temp_id,
        chunk_index: int,
        chunk_buffer: bytearray,
        chunk_meta_list: list,
        chunk_paths: list
    ):
        """
        Write a chunk to staging. Accepts a bytearray but immediately writes
        it without making a second copy — bytearray is writable directly.
        """
        chunk_size = len(chunk_buffer)
        chunk_checksum = hashlib.sha256(chunk_buffer).hexdigest()

        stage_dir = self.staging_dir / temp_id
        stage_dir.mkdir(exist_ok=True, parents=True)
        chunk_path = stage_dir / f"{chunk_checksum}.dat"

        # Write directly from bytearray — no bytes() copy needed
        buf_view = memoryview(chunk_buffer)
        def write_chunk():
            with open(chunk_path, 'wb') as f:
                f.write(buf_view)

        await asyncio.to_thread(write_chunk)

        chunk_meta_list.append({
            'chunk_index': chunk_index,
            'size_bytes': chunk_size,
            'chunk_hash': chunk_checksum,
        })
        chunk_paths.append(chunk_path)
        print(f"Chunk {chunk_index}: {chunk_size:,} bytes → {chunk_checksum[:12]}...")
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
        """Atomically reserve space for an upload"""
        self._update_capacity_cached()

        async with self.reservation_lock:
            truly_available = self.free_capacity_gb - self.reserved_space_gb

            if size_gb > truly_available:
                return False

            self.reserved_space_gb += size_gb
            return True

    async def _release_reservation(self, size_gb: float):
        """Release a space reservation (called on upload failure)"""
        async with self.reservation_lock:
            self.reserved_space_gb -= size_gb
            if self.reserved_space_gb < 0:
                self.reserved_space_gb = 0

    async def _commit_reservation(self, size_gb: float):
        """Commit a space reservation (called on upload success)"""
        async with self.reservation_lock:
            self.reserved_space_gb -= size_gb
            if self.reserved_space_gb < 0:
                self.reserved_space_gb = 0

    async def _bootstrap_static_peers(self):
        """
        Register peers supplied via the TOSSIT_PEER_URLS environment variable.

        This is the Docker / non-mDNS discovery path. mDNS still runs in
        parallel — whichever discovers a peer first wins, duplicates are
        safely ignored because both paths call _on_node_discovered which
        keys on node_id.

        TOSSIT_PEER_URLS accepts a comma-separated list of base URLs:
            TOSSIT_PEER_URLS=http://node1:8000,http://node2:8000

        Each URL is queried for /api/health to obtain the node_id, name,
        and capacity.  Unreachable peers are skipped and retried by the
        normal periodic health check loop.
        """
        raw = os.environ.get('TOSSIT_PEER_URLS', '').strip()
        if not raw:
            return

        peer_urls = [u.strip().rstrip('/') for u in raw.split(',') if u.strip()]
        if not peer_urls:
            return

        print(f"Static peer bootstrap: {len(peer_urls)} URL(s) from TOSSIT_PEER_URLS")

        session = await self._get_http_session()

        for url in peer_urls:
            # Retry a few times — peers in the same compose cluster may still
            # be starting up when we first try.
            for attempt in range(5):
                try:
                    async with session.get(
                        f"{url}/api/health",
                        timeout=aiohttp.ClientTimeout(total=3.0)
                    ) as resp:
                        if resp.status != 200:
                            raise ValueError(f"HTTP {resp.status}")

                        data = await resp.json()

                        # Validate minimum required fields
                        peer_node_id = data.get('node_id')
                        peer_name    = data.get('node_name', 'unknown')
                        if not peer_node_id:
                            print(f"Static peer {url}: missing node_id in /api/health response")
                            break

                        # Skip ourselves
                        if peer_node_id == self.node_id:
                            break

                        # Parse host + port from the URL
                        from urllib.parse import urlparse
                        parsed   = urlparse(url)
                        peer_ip  = parsed.hostname or '127.0.0.1'
                        peer_port = parsed.port or 8000

                        node_info = {
                            'node_id':       peer_node_id,
                            'node_name':     peer_name,
                            'ip_address':    peer_ip,
                            'port':          peer_port,
                            'storage_gb':    data.get('total_capacity_gb', 0),
                            'free_capacity_gb': data.get('free_capacity_gb', 0),
                            'cluster_id':    data.get('cluster_id', self.config.cluster_id),
                            'discovered_at': datetime.now(timezone.utc).isoformat(),
                            'service_name':  f"static-{peer_node_id}",
                        }

                        await self._on_node_discovered(node_info)
                        print(f"Static peer registered: {peer_name} @ {peer_ip}:{peer_port}")
                        break  # success

                except Exception as e:
                    wait = 2 ** attempt
                    if attempt < 4:
                        print(f"Static peer {url} not ready (attempt {attempt+1}/5), retrying in {wait}s: {e}")
                        await asyncio.sleep(wait)
                    else:
                        print(f"Static peer {url} unreachable after 5 attempts: {e}")

    async def _replicate_file_chunks(self, file_id: int):
        """
        Replicate file chunks to peer nodes.
        - N=2 for clusters with 2-4 nodes
        - N=3 for clusters with 5+ nodes
        """
        try:
            import base64

            if not self.peer_nodes:
                print("No peers available for replication")
                return

            total_nodes = 1 + len(self.peer_nodes)
            if total_nodes <= 4:
                REPLICATION_FACTOR = min(2, len(self.peer_nodes))
            else:
                REPLICATION_FACTOR = min(3, len(self.peer_nodes))

            print(f"Starting replication (factor: {REPLICATION_FACTOR}, cluster size: {total_nodes})")

            target_nodes = self._select_replica_nodes(REPLICATION_FACTOR)

            if not target_nodes:
                print("No suitable replica nodes found")
                return

            db = self.SessionLocal()
            try:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file_record:
                    print(f"File {file_id} not found in database")
                    return

                file_metadata = {
                    'filename': file_record.filename,
                    'total_size_bytes': file_record.total_size_bytes,
                    'chunk_size_bytes': file_record.chunk_size_bytes,
                    'total_chunks': file_record.total_chunks,
                    'checksum_sha256': file_record.checksum_sha256,
                    'uploaded_by': file_record.uploaded_by
                }

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
                print(f"No chunks found for file {file_id}")
                return

            CHUNK_SIZE = 64 * 1024 * 1024

            for target_node_id, target_info in target_nodes:
                target_url = f"http://{target_info['ip_address']}:{target_info['port']}"
                print(f"Replicating to: {target_info['node_name']} ({target_info['ip_address']})")

                MAX_RETRIES = 3
                RETRY_DELAY_BASE = 2

                chunk_status = {
                    chunk['chunk_index']: {'sent': False, 'acked': False, 'retries': 0}
                    for chunk in chunk_records
                }

                print(f"Initial send: {len(chunk_records)} chunks to {target_info['node_name']}")
                for i, chunk in enumerate(chunk_records):
                    success = await self._send_chunk_from_disk(
                        file_id, chunk, chunk_records,
                        file_metadata,
                        target_url, target_info, chunk_status, CHUNK_SIZE
                    )
                    await asyncio.sleep(0)

                for retry_round in range(MAX_RETRIES):
                    failed_chunks = [
                        chunk for chunk in chunk_records
                        if not chunk_status[chunk['chunk_index']]['acked']
                        and chunk_status[chunk['chunk_index']]['retries'] < MAX_RETRIES
                    ]

                    if not failed_chunks:
                        break

                    retry_delay = RETRY_DELAY_BASE ** (retry_round + 1)
                    print(f"Retry round {retry_round + 1}/{MAX_RETRIES}: {len(failed_chunks)} chunks failed, waiting {retry_delay}s...")
                    await asyncio.sleep(retry_delay)

                    for chunk in failed_chunks:
                        chunk_status[chunk['chunk_index']]['retries'] += 1
                        success = await self._send_chunk_from_disk(
                            file_id, chunk, chunk_records,
                            file_metadata,
                            target_url, target_info, chunk_status, CHUNK_SIZE
                        )
                        await asyncio.sleep(0)

                acked_count = sum(1 for s in chunk_status.values() if s['acked'])
                failed_count = len(chunk_records) - acked_count

                if failed_count > 0:
                    print(f"Replication incomplete to {target_info['node_name']}: {acked_count}/{len(chunk_records)} chunks ACKed, {failed_count} failed after {MAX_RETRIES} retries")
                else:
                    print(f"Replication complete to {target_info['node_name']}: {acked_count}/{len(chunk_records)} chunks ACKed")

            print(f"All replications complete: {len(chunk_records)} chunks → {len(target_nodes)} nodes")

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
        """
        idx = chunk['chunk_index']
        checksum = chunk['chunk_hash']

        try:
            import base64

            async with self._replication_semaphore:
                chunk_path = self.chunks_dir / f"{checksum}.dat"
                if not chunk_path.exists():
                    print(f"Chunk {idx} not found: {checksum[:12]}...")
                    return False

                chunk_data = await asyncio.to_thread(chunk_path.read_bytes)

                chunk_data_b64 = base64.b64encode(chunk_data).decode('utf-8')

                payload = {
                    'file_id': file_id,
                    'chunk_index': idx,
                    'chunk_data': chunk_data_b64,
                    'checksum': checksum,
                    'node_name': self.config.node_name,
                    'timestamp': time.time()
                }

                if file_metadata:
                    payload['file_metadata'] = file_metadata

                signed_payload = self.identity.sign_json(payload)

                session = await self._get_http_session()
                async with session.post(
                    f"{target_url}/api/internal/replicate_chunk",
                    json=signed_payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status == 200:
                        ack_data = await response.json()

                        if (ack_data.get('status') == 'ack' and
                            ack_data.get('chunk_index') == idx and
                            ack_data.get('checksum') == checksum):

                            chunk_status[idx]['sent'] = True
                            chunk_status[idx]['acked'] = True
                            print(f"Chunk {idx + 1}/{len(chunk_records)} ACKed → {target_info['node_name']}")
                            return True
                        else:
                            print(f"Chunk {idx} invalid ACK response")
                            return False

                    elif response.status == 401:
                        print(f"Chunk {idx} rejected: Invalid signature")
                        return False
                    else:
                        print(f"Chunk {idx} failed: HTTP {response.status}")
                        return False

        except Exception as e:
            print(f"Chunk {idx} error: {e}")
            return False

    def _select_replica_nodes(self, N: int) -> list:
        """
        Select N replica nodes using GREEDY strategy: pick nodes with most free space.
        Ties are broken by latency (prefer distant nodes for better fault tolerance).
        """
        if not self.peer_nodes:
            return []

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

        def sort_key(peer):
            node_id, node_info = peer
            free_gb = node_info.get('free_capacity_gb', node_info.get('storage_gb', 0))
            latency = node_info.get('latency_ms', 0)
            return (-free_gb, -latency)

        sorted_peers = sorted(online_peers, key=sort_key)
        selected = sorted_peers[:N]

        if len(selected) < N:
            print(f"Only {len(selected)} replica nodes available (wanted {N})")

        self._log_replica_selection(selected)
        return selected

    def _log_replica_selection(self, selected_replicas: list):
        """Log information about selected replica nodes"""
        if not selected_replicas:
            return

        print(f"Selected {len(selected_replicas)} replica nodes:")

        latencies = []
        far_count = medium_count = close_count = unknown_count = 0

        for node_id, node_info in selected_replicas:
            free_gb = node_info.get('free_capacity_gb', node_info.get('storage_gb', 0))
            total_gb = node_info.get('storage_gb', 0)
            latency = node_info.get('latency_ms', 0)
            utilization = ((total_gb - free_gb) / total_gb * 100) if total_gb > 0 else 0

            print(f"• {node_info['node_name']}: {free_gb:.1f}GB free / {total_gb:.1f}GB total ({utilization:.1f}% used), {latency:.1f}ms latency")

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

        if latencies:
            min_lat = min(latencies)
            max_lat = max(latencies)
            avg_lat = sum(latencies) / len(latencies)
            print(f"Latency range: {min_lat:.1f}ms - {max_lat:.1f}ms (avg: {avg_lat:.1f}ms)")

    async def _decrement_assignment(self, node_id: str = "self"):
        """Decrement the assignment counter when an upload completes."""
        async with self._assigned_uploads_lock:
            if node_id in self._assigned_uploads:
                self._assigned_uploads[node_id] = max(0, self._assigned_uploads[node_id] - 1)

    async def _notify_leader_upload_complete(self):
        """Tell the leader that a delegated upload finished on this node."""
        if not self.raft or self.raft.is_leader():
            return

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
                pass
        except Exception:
            pass

    async def _reconcile_on_promotion(self):
        """
        METADATA RECONCILIATION — run when this node becomes leader.
        Queries every peer's inventory and merges with local database.
        """
        print()
        print("=" * 60)
        print("METADATA RECONCILIATION — new leader recovery")
        print("=" * 60)

        await asyncio.sleep(3.0)

        if not self.raft or not self.raft.is_leader():
            print("Lost leadership during reconciliation wait — aborting")
            return

        # STEP 1: Clean stale delegation placeholders
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

        # STEP 2: Scan local disk for orphaned chunks
        orphaned_hashes = []

        if self.chunks_dir.exists():
            db = self.SessionLocal()
            try:
                for dat_file in self.chunks_dir.glob("*.dat"):
                    chunk_hash = dat_file.stem
                    if len(chunk_hash) == 64:
                        refs = db.query(FileChunk).filter(
                            FileChunk.chunk_hash == chunk_hash
                        ).count()
                        if refs == 0:
                            orphaned_hashes.append(chunk_hash)
            finally:
                db.close()

        if orphaned_hashes:
            print(f"Step 2: Found {len(orphaned_hashes)} orphaned chunks on local disk")
        else:
            print(f"Step 2: No orphaned chunks on local disk")

        # STEP 3: Query peer inventories
        peer_inventories = {}

        for node_id, peer_info in list(self.peer_nodes.items()):
            if self.health_monitor:
                status = self.health_monitor.get_peer_status(node_id)
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

        # STEP 4: Import metadata for unknown files
        files_imported = 0
        files_already_known = 0

        for node_id, inventory in peer_inventories.items():
            peer_name = inventory.get("node_name", node_id[:8])

            for file_info in inventory.get("files", []):
                file_id = file_info["file_id"]

                if file_info.get("checksum_sha256") == "pending_delegation":
                    continue

                if not file_info.get("is_complete", False):
                    continue

                db = self.SessionLocal()
                try:
                    existing = db.query(FileModel).filter(FileModel.id == file_id).first()

                    if existing and existing.checksum_sha256 != "pending_delegation":
                        files_already_known += 1
                        continue

                    async with self._db_write_semaphore:
                        db2 = self.SessionLocal()
                        try:
                            existing2 = db2.query(FileModel).filter(FileModel.id == file_id).first()

                            if existing2:
                                existing2.filename = file_info["filename"]
                                existing2.total_size_bytes = file_info["total_size_bytes"]
                                existing2.chunk_size_bytes = file_info.get("chunk_size_bytes", 64 * 1024 * 1024)
                                existing2.total_chunks = file_info["total_chunks"]
                                existing2.checksum_sha256 = file_info["checksum_sha256"]
                                existing2.uploaded_by = file_info.get("uploaded_by", "reconciled")
                                existing2.is_complete = True
                                existing2.is_replicated = False
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

                            for chunk_info in file_info.get("chunks_on_disk", []):
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

        print(f"Step 4: Imported {files_imported} files from peers ({files_already_known} already known)")

        # STEP 5: Trigger re-replication for files only on one node
        if len(self.peer_nodes) > 0:
            await asyncio.sleep(1.0)

            replication_needed = 0
            db = self.SessionLocal()
            try:
                unreplicated = db.query(FileModel).filter(
                    FileModel.is_complete == True,
                    FileModel.is_replicated == False,
                ).all()

                for f in unreplicated:
                    file_chunks = db.query(FileChunk).filter(FileChunk.file_id == f.id).all()
                    local_on_disk = sum(
                        1 for fc in file_chunks
                        if (self.chunks_dir / f"{fc.chunk_hash}.dat").exists()
                    )
                    has_local = local_on_disk == f.total_chunks

                    if has_local:
                        asyncio.create_task(self._replicate_file_chunks(f.id))
                        replication_needed += 1
                    else:
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
        Called when a new node joins the cluster to replicate existing files.
        """
        if not self.raft or not self.raft.is_leader():
            print("Not leader, skipping redistribution")
            return

        if len(self.peer_nodes) == 0:
            print("No peers available for redistribution")
            return

        db = self.SessionLocal()
        try:
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

            total_nodes = 1 + len(self.peer_nodes)
            if total_nodes <= 4:
                target_replication = 2
            else:
                target_replication = 3

            files_to_replicate = []
            for file_id, filename, current_replicas, total_chunks in files_needing_replication:
                needs_replication = current_replicas < target_replication

                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                already_replicated = file_record.is_replicated if file_record else False

                if needs_replication and (not already_replicated or current_replicas == 0):
                    files_to_replicate.append((file_id, filename, current_replicas, total_chunks))
                elif already_replicated and needs_replication:
                    print(f"File {filename} marked as replicated but only has {current_replicas}/{target_replication} replicas")

            if not files_to_replicate:
                print("All files properly replicated")
                return

            print(f"Redistributing {len(files_to_replicate)} files to new cluster topology...")

            for file_id, filename, current_replicas, total_chunks in files_to_replicate:
                print(f"Replicating: {filename} (currently {current_replicas} replicas, want {target_replication})")

                chunks = db.query(FileChunk).filter(
                    FileChunk.file_id == file_id
                ).order_by(FileChunk.chunk_index).all()

                if not chunks:
                    print(f"No chunks found for file {file_id}")
                    continue

                try:
                    await self._replicate_file_chunks(file_id)
                    print(f"Replication scheduled for {filename}")
                except Exception as e:
                    print(f"Failed to replicate {filename}: {e}")

            print(f"✅ Redistribution complete")

        except Exception as e:
            print(f"Redistribution failed: {e}")
        finally:
            db.close()

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

        @self.app.middleware("http")
        async def add_no_cache_headers(request, call_next):
            response = await call_next(request)
            if request.url.path.startswith("/api/"):
                response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
                response.headers["Pragma"] = "no-cache"
                response.headers["Expires"] = "0"
            return response

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
            """Show upload queue status"""
            active = 3 - self._upload_semaphore._value
            return {
                "max_concurrent_uploads": 3,
                "active_uploads": active,
                "available_slots": self._upload_semaphore._value,
                "reserved_space_gb": round(self.reserved_space_gb, 2),
                "free_capacity_gb": round(self.free_capacity_gb, 2),
            }

        @self.app.get("/api/raft/ping")
        async def raft_ping():
            """Zero-I/O liveness probe for pre-vote checks."""
            return {"alive": True, "node_id": self.node_id, "t": time.time()}

        @self.app.post("/api/raft/heartbeat")
        async def receive_raft_heartbeat(request: dict):
            """Receive heartbeat from leader"""
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")

            if 'signature' not in request or 'public_key' not in request:
                raise HTTPException(401, "Unsigned messages not accepted")

            verified, message, sender_node_id = self.trust_store.verify_message(request)
            if not verified:
                print(f"Rejected heartbeat: Invalid signature")
                raise HTTPException(401, "Invalid signature")

            leader_id = sender_node_id
            term = message['term']
            leader_name = message.get('node_name', 'unknown')
            print(f"Verified heartbeat from {leader_name} ({sender_node_id[:8]}...)")

            await self.raft.receive_heartbeat(leader_id, term)
            return {"status": "ok", "follower_term": self.raft.current_term}

        @self.app.post("/api/raft/vote")
        async def receive_raft_vote_request(request: dict):
            """Receive vote request from candidate"""
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")

            if 'signature' not in request or 'public_key' not in request:
                raise HTTPException(401, "Unsigned messages not accepted")

            verified, message, sender_node_id = self.trust_store.verify_message(request)
            if not verified:
                print(f"Rejected vote request: Invalid signature")
                raise HTTPException(401, "Invalid signature")

            candidate_id = sender_node_id
            term = message['term']
            candidate_name = message.get('node_name', 'unknown')
            print(f"Verified vote request from {candidate_name} ({sender_node_id[:8]}...)")

            vote_granted = await self.raft.request_vote(candidate_id, term)
            return {"vote_granted": vote_granted}

        @self.app.get("/api/upload/assign")
        async def assign_upload(filename: str, size: int):
            """
            PROACTIVE UPLOAD LOAD BALANCER
            Distributes uploads evenly across all nodes in the cluster.
            """
            my_url = f"http://{self.local_ip}:{self.config.port}"

            is_current_leader = self.raft and self.raft.is_leader()
            if not is_current_leader:
                # Always tell the client to upload to the node it already reached.
                # That node will proxy internally to the leader — we never hand out
                # internal Docker/LAN IPs to the browser.
                my_url = f"http://{self.local_ip}:{self.config.port}"
                return {"upload_to": my_url, "redirect_to_leader": False, "delegated": False, "file_id": None}

            size_gb = size / (1024 ** 3)

            async with self._assigned_uploads_lock:
                candidates = []

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

                for node_id, node_info in self.peer_nodes.items():
                    if self.health_monitor:
                        status = self.health_monitor.get_peer_status(node_id)
                        if status != "online":
                            continue

                    peer_free = node_info.get('free_capacity_gb', 0)
                    if peer_free < size_gb:
                        continue

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

                candidates.sort(key=lambda c: (c["active"], c["is_leader"]))
                winner = candidates[0]

                self._assigned_uploads[winner["id"]] = winner["active"] + 1

            if winner["id"] == "self":
                total_cluster = sum(self._assigned_uploads.values())
                print(f"Assign '{filename}' → {winner['name']} (self, {winner['active']+1} active, cluster total: {total_cluster})")
                return {
                    "upload_to": my_url,
                    "delegated": False,
                    "file_id": None,
                    "assigned_node": winner["name"],
                }

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
            """Decrement assignment counter when a delegated upload finishes."""
            node_id = request.get("node_id", "self")
            async with self._assigned_uploads_lock:
                if node_id in self._assigned_uploads:
                    self._assigned_uploads[node_id] = max(0, self._assigned_uploads[node_id] - 1)
            return {"status": "ok"}

        @self.app.post("/api/upload")
        async def upload_file(
            file: UploadFile = FastAPIFile(...),
            delegated_file_id: Optional[int] = Query(None, description="Pre-assigned file ID from leader for delegated uploads")
        ):
            """
            STREAMING Upload with constant memory usage.

            Phase 1: Stream to disk (NO DATABASE - fully parallel)
            Phase 2: Batch DB write under semaphore (<100ms lock)
            Phase 3: Rename staging files to permanent content-addressed names
            Memory usage: ~64MB constant (one chunk buffer, regardless of file size)
            """
            is_delegated = delegated_file_id is not None

            if is_delegated:
                print(f"Delegated upload received: {file.filename} (file_id={delegated_file_id})")
            else:
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
                        peer = self.peer_nodes[leader_id]
                        leader_url = f"http://{peer['ip_address']}:{peer['port']}/api/upload"
                        # Proxy the upload to the leader rather than redirecting the browser.
                        # A redirect to an internal IP (172.18.x.x) would be unreachable
                        # from outside the Docker network.
                        print(f"Proxying upload to leader at {leader_url}")
                        # Stream directly — never buffer the whole file in RAM.
                        file.file.seek(0)
                        import aiohttp as _aiohttp
                        async with _aiohttp.ClientSession() as _session:
                            form = _aiohttp.FormData()
                            form.add_field(
                                'file',
                                file.file,
                                filename=file.filename,
                                content_type=file.content_type or 'application/octet-stream'
                            )
                            async with _session.post(
                                leader_url, data=form,
                                timeout=_aiohttp.ClientTimeout(
                                    total=None,        # no overall cap
                                    connect=10,
                                    sock_read=300      # 5 min stall timeout
                                )
                            ) as _resp:
                                _body_text = await _resp.text()
                                if _resp.status != 200:
                                    raise HTTPException(_resp.status, detail=_body_text)
                                import json as _json
                                try:
                                    return _json.loads(_body_text)
                                except Exception:
                                    raise HTTPException(500, detail=f"Leader returned unexpected response: {_body_text[:200]}")
                    else:
                        raise HTTPException(503, "No leader available - cluster electing")

                if recently_was_leader:
                    print(f"Accepting upload during leadership grace period ({time.time() - self._leadership_lost_at:.1f}s since step-down)")

            # 8 MB chunks — enough for content-addressing without
            # holding large buffers in RAM on low-end hardware.
            CHUNK_SIZE = 8 * 1024 * 1024
            READ_SIZE  = 64 * 1024   # 64 KB reads — good balance of syscall overhead vs RAM

            file.file.seek(0, 2)
            file_size = file.file.tell()
            file.file.seek(0)
            file_size_gb = file_size / (1024 ** 3)

            if file_size > MAX_UPLOAD_SIZE_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f"File too large: {file_size / (1024**3):.2f}GB exceeds "
                           f"max {MAX_UPLOAD_SIZE_BYTES / (1024**3):.0f}GB"
                )

            space_reserved = await self._reserve_space(file_size_gb)
            if not space_reserved:
                self._update_capacity()
                available = self.free_capacity_gb - self.reserved_space_gb
                raise HTTPException(
                    status_code=507,
                    detail=f"Insufficient storage: need {file_size_gb:.2f}GB, available {available:.2f}GB"
                )

            import uuid
            temp_id = uuid.uuid4().hex[:12]

            print(f"Upload accepted: {file.filename} ({file_size:,} bytes)")

            chunk_paths_temp = []
            chunk_meta_list = []

            try:
                # PHASE 1: Stream directly to disk, one chunk at a time.
                # Peak RAM = one 8 MB bytearray — constant regardless of file size.
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
                            chunk_buffer = bytearray()   # new alloc — releases old memory immediately

                            if chunk_index % 20 == 0:
                                print(f"Progress: {chunk_index} chunks ({total_bytes_read / (1024**3):.2f} GB)")

                        await asyncio.sleep(0)  # yield every read — keeps heartbeats alive

                    if chunk_buffer:
                        await self._write_chunk_to_disk(
                            temp_id, chunk_index,
                            chunk_buffer, chunk_meta_list, chunk_paths_temp
                        )
                        chunk_index += 1

                file_checksum = file_hash.hexdigest()

                # PHASE 2: Atomic DB commit
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
                            self._ensure_chunk(db, meta['chunk_hash'], meta['size_bytes'])

                            file_chunk = FileChunk(
                                file_id=file_id,
                                chunk_index=meta['chunk_index'],
                                chunk_hash=meta['chunk_hash'],
                            )
                            db.add(file_chunk)

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

                # PHASE 3: Move staging → chunks/
                for meta in chunk_meta_list:
                    staged_path = self.staging_dir / temp_id / f"{meta['chunk_hash']}.dat"
                    final_path = self.chunks_dir / f"{meta['chunk_hash']}.dat"
                    if final_path.exists():
                        staged_path.unlink(missing_ok=True)
                    elif staged_path.exists():
                        await asyncio.to_thread(staged_path.rename, final_path)

                stage_dir = self.staging_dir / temp_id
                if stage_dir.exists():
                    import shutil as _shutil
                    _shutil.rmtree(stage_dir, ignore_errors=True)

                await self._commit_reservation(file_size_gb)

                print(f"Upload complete: {file.filename}")
                print(f"File ID: {file_id}, Size: {total_bytes_read:,} bytes ({chunk_index} chunks)")
                print(f"Checksum: {file_checksum[:16]}...")

                if is_delegated:
                    asyncio.create_task(self._notify_leader_upload_complete())
                else:
                    await self._decrement_assignment("self")

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

        @self.app.post("/api/internal/replicate_chunk")
        async def receive_chunk_replica(request: dict):
            """
            Receive chunk replica from leader.

            Phase 1: Verify signature + decode + checksum (no DB)
            Phase 2: Write chunk to disk (no DB)
            Phase 3: Batch DB write under semaphore (<50ms)
            """
            # PHASE 1: Verify and extract
            if 'signature' not in request or 'public_key' not in request:
                raise HTTPException(401, "Unsigned messages not accepted")

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

            # IMPLICIT HEARTBEAT: Replication data from the leader resets election timer.
            if self.raft and self.raft.leader_id == sender_node_id:
                self.raft.last_heartbeat = time.time()

            import base64
            chunk_data = base64.b64decode(chunk_data_b64)

            import hashlib
            actual_checksum = hashlib.sha256(chunk_data).hexdigest()
            if actual_checksum != chunk_checksum:
                raise HTTPException(400, "Checksum mismatch")

            # PHASE 2: Write to content-addressed store
            chunk_path = self.chunks_dir / f"{chunk_checksum}.dat"
            if not chunk_path.exists():
                await asyncio.to_thread(chunk_path.write_bytes, chunk_data)

            # PHASE 3: DB write under semaphore
            async with self._db_write_semaphore:
                db = self.SessionLocal()
                try:
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

                    self._ensure_chunk(db, chunk_checksum, len(chunk_data), increment_ref=False)

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

        @self.app.get("/api/download/{file_id}")
        async def download_file(file_id: int):
            """
            Download a file — streams chunks directly from disk.
            Content-addressed: looks up FileChunk mappings to find chunk hashes,
            then reads from chunks/{hash}.dat in order.
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

                file_chunks = db.query(FileChunk).filter(
                    FileChunk.file_id == file_id
                ).order_by(FileChunk.chunk_index).all()

                chunk_paths = []
                for fc in file_chunks:
                    chunk_path = self.chunks_dir / f"{fc.chunk_hash}.dat"
                    if not chunk_path.exists():
                        raise HTTPException(
                            status_code=500,
                            detail=f"Missing chunk {fc.chunk_index} ({fc.chunk_hash[:12]}...) for file {file_id}"
                        )
                    chunk_paths.append(chunk_path)

            finally:
                db.close()

            async def chunk_streamer():
                for chunk_path in chunk_paths:
                    data = await asyncio.to_thread(chunk_path.read_bytes)
                    yield data
                    await asyncio.sleep(0)

            return StreamingResponse(
                chunk_streamer(),
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f'attachment; filename="{filename}"',
                    "Content-Length": str(total_size),
                }
            )

        @self.app.delete("/api/files/{file_id}")
        async def delete_file(file_id: int):
            """Delete a file. Only removes chunk data if no other file references the chunk."""
            db = self.SessionLocal()
            try:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file_record:
                    raise HTTPException(status_code=404, detail="File not found")

                filename = file_record.filename

                file_chunks = db.query(FileChunk).filter(FileChunk.file_id == file_id).all()
                chunk_hashes = [fc.chunk_hash for fc in file_chunks]

                db.delete(file_record)
                await db_flush_with_retry(db)

                gc_count = 0
                for ch in chunk_hashes:
                    if self._decrement_chunk_ref(db, ch):
                        gc_count += 1

                await db_commit_with_retry(db)

                print(f"Deleted: {filename} (ID: {file_id}, {len(chunk_hashes)} mappings, {gc_count} chunks garbage collected)")

                if self.peer_nodes:
                    asyncio.create_task(
                        self._delete_remote_replicas(file_id, len(chunk_hashes), chunk_hashes)
                    )

                return {"message": "File deleted successfully"}
            finally:
                db.close()

        @self.app.post("/api/internal/delete_chunks")
        async def receive_delete_request(request: dict):
            """Receive chunk deletion request from leader"""
            db = self.SessionLocal()
            try:
                if 'signature' not in request or 'public_key' not in request:
                    raise HTTPException(401, "Unsigned messages not accepted")

                verified, message, sender_node_id = self.trust_store.verify_message(request)
                if not verified:
                    raise HTTPException(401, "Invalid signature")

                file_id = message['file_id']
                chunk_hashes = message.get('chunk_hashes', [])

                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if file_record:
                    file_chunks = db.query(FileChunk).filter(FileChunk.file_id == file_id).all()
                    file_chunk_hashes = [fc.chunk_hash for fc in file_chunks]

                    db.delete(file_record)
                    await db_flush_with_retry(db)

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

        @self.app.get("/api/health")
        async def health_check():
            self._update_capacity()

            return {
                "status": "healthy",
                "node_id": self.node_id,
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
            """Lightweight file inventory for leader reconciliation."""
            db = self.SessionLocal()
            try:
                files = db.query(FileModel).all()
                inventory = []

                for f in files:
                    file_chunks = db.query(FileChunk).filter(
                        FileChunk.file_id == f.id
                    ).order_by(FileChunk.chunk_index).all()

                    chunks_on_disk = []
                    for fc in file_chunks:
                        chunk_path = self.chunks_dir / f"{fc.chunk_hash}.dat"
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

                all_chunk_hashes = []
                if self.chunks_dir.exists():
                    for dat_file in self.chunks_dir.glob("*.dat"):
                        chunk_hash = dat_file.stem
                        if len(chunk_hash) == 64:
                            all_chunk_hashes.append(chunk_hash)

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

                total_nodes = 1 + len(self.peer_nodes)

                online_nodes = 1
                for node_id in self.peer_nodes.keys():
                    if self.health_monitor:
                        peer_status = self.health_monitor.get_peer_status(node_id)
                        if peer_status == "online":
                            online_nodes += 1
                    else:
                        online_nodes += 1

                print(f"[DEBUG] Cluster stats: {online_nodes}/{total_nodes} nodes online")

                raw_capacity = self.total_capacity_gb
                for n in self.peer_nodes.values():
                    raw_capacity += n["storage_gb"]

                if total_nodes == 1:
                    replication_factor = 1
                elif total_nodes <= 4:
                    replication_factor = 2
                else:
                    replication_factor = 3

                usable_capacity = raw_capacity / replication_factor

                unique_data_result = db.execute(
                    text("SELECT COALESCE(SUM(total_size_bytes), 0) FROM files")
                ).fetchone()
                unique_data_bytes = unique_data_result[0] if unique_data_result else 0
                unique_data_gb = round(unique_data_bytes / (1024 ** 3), 2)

                total_replicas = db.execute(
                    text("SELECT COUNT(*) FROM chunk_locations")
                ).fetchone()[0]

                total_chunks = db.execute(
                    text("SELECT COUNT(*) FROM file_chunks")
                ).fetchone()[0]

                file_count = db.query(FileModel).count()

                actual_storage_used = db.execute(
                    text("""
                        SELECT COALESCE(SUM(c.size_bytes), 0)
                        FROM chunk_locations cl
                        JOIN chunks c ON c.chunk_hash = cl.chunk_hash
                    """)
                ).fetchone()[0]
                actual_storage_used_gb = round(actual_storage_used / (1024 ** 3), 2)

                free_capacity = (raw_capacity - actual_storage_used_gb) / replication_factor

                return {
                    "total_nodes": total_nodes,
                    "online_nodes": online_nodes,
                    "total_files": file_count,
                    "total_chunks": total_chunks,
                    "total_replicas": total_replicas,
                    "avg_replicas_per_chunk": round(total_replicas / total_chunks, 1) if total_chunks > 0 else 0,

                    "raw_capacity_gb": round(raw_capacity, 2),
                    "total_capacity_gb": round(usable_capacity, 2),
                    "unique_data_gb": unique_data_gb,
                    "actual_storage_used_gb": actual_storage_used_gb,
                    "free_capacity_gb": round(max(0, free_capacity), 2),
                    "utilization_percent": round((unique_data_gb / usable_capacity * 100), 1) if usable_capacity > 0 else 0,

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

            leader_id = self.raft.get_leader_id() if self.raft else None
            for idx, (node_id, node_info) in enumerate(self.peer_nodes.items(), start=2):
                peer_is_leader = (node_id == leader_id) if leader_id else False

                peer_status = "online"
                if self.health_monitor:
                    peer_status = self.health_monitor.get_peer_status(node_id)

                last_seen_ago = 0
                if self.health_monitor and node_id in self.health_monitor.peer_last_seen:
                    last_seen_ago = int(time.time() - self.health_monitor.peer_last_seen[node_id])

                peer_free_gb = node_info.get('free_capacity_gb', node_info['storage_gb'])
                peer_total_gb = node_info['storage_gb']

                print(f"[DEBUG] Node {node_info['node_name']}: status={peer_status}, last_seen={last_seen_ago}s ago, free={peer_free_gb:.1f}GB")

                nodes.append({
                    "id": idx,
                    "name": node_info['node_name'],
                    "ip_address": node_info['ip_address'],
                    "port": node_info['port'],
                    "total_capacity_gb": peer_total_gb,
                    "free_capacity_gb": peer_free_gb,
                    "cpu_score": 1.0,
                    "priority_score": 1.0,
                    "status": peer_status,
                    "is_brain": False,
                    "is_leader": peer_is_leader,
                    "raft_state": "follower" if not peer_is_leader else "leader",
                    "last_heartbeat": node_info['discovered_at'],
                    "last_heartbeat_seconds_ago": last_seen_ago,
                    "latency_ms": node_info.get('latency_ms', None)
                })

            # Deduplicate by node name — mDNS and static peer discovery can
            # both register the same node under different IPs/node_ids.
            # Keep the entry with the most recent heartbeat (lowest last_seen seconds).
            seen_names = {}
            for node in nodes:
                name = node['name']
                if name not in seen_names:
                    seen_names[name] = node
                else:
                    # Prefer the one seen most recently
                    if node['last_heartbeat_seconds_ago'] < seen_names[name]['last_heartbeat_seconds_ago']:
                        seen_names[name] = node

            deduped = list(seen_names.values())
            # Re-assign sequential IDs
            for i, node in enumerate(deduped, start=1):
                node['id'] = i

            return deduped

        @self.app.get("/api/files")
        async def get_files():
            db = self.SessionLocal()
            try:
                files = db.query(FileModel).order_by(FileModel.created_at.desc()).all()

                result = []
                for f in files:
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
        """Send delete requests to all peer nodes to remove replicated chunks."""
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

                signed_payload = self.identity.sign_json(delete_message)

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
        print(f"Data root:     {DATA_ROOT}")
        print("="*60)
        print()

        await self._verify_chunks_on_boot()

        self.raft = ClusterRaft(
            node_id=self.node_id,
            node_name=self.config.node_name,
            port=self.config.port,
            data_dir=str(DB_DIR),
            identity=self.identity,
            trust_store=self.trust_store,
            on_become_leader=self._on_became_leader,
            on_lose_leadership=self._on_lost_leadership,
        )
        await self.raft.start()

        self.health_monitor = NodeHealthMonitor(
            timeout_seconds=60.0,
            check_interval=10.0,
            on_node_offline=self._on_node_went_offline
        )
        await self.health_monitor.start()

        self.peer_refresh_task = asyncio.create_task(self._periodic_peer_refresh())

        # If static peer URLs are configured (Docker / known-topology deployments),
        # skip mDNS entirely — no risk of double-registering the same node under
        # two different IPs (hostname vs resolved IP).
        # mDNS is only used for true zero-config LAN discovery (bare-metal homelab).
        _use_mdns = not os.environ.get('TOSSIT_PEER_URLS', '').strip()

        if _use_mdns:
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
            print("Discovery: mDNS enabled (no static peers configured)")
        else:
            print("Discovery: mDNS skipped — using static peer URLs")
            asyncio.create_task(self._bootstrap_static_peers())

        registry_url = os.getenv('TOSSIT_REGISTRY_URL')
        if registry_url:
            self.registry_client = RegistryClient(
                registry_url=registry_url,
                cluster_id=self.config.cluster_id,
                node_name=self.config.node_name,
                port=self.config.port
            )
            print(f"Registry configured: {registry_url}")

            registry_ok = await self.registry_client.test_connection()
            if registry_ok:
                print("Registry service reachable")
            else:
                print("Warning: Registry service not reachable (will retry)")

            asyncio.create_task(self._update_registry_stats_loop())
        else:
            print("No registry configured (set TOSSIT_REGISTRY_URL to enable)")

        config = uvicorn.Config(
            self.app,
            host="0.0.0.0",
            port=self.config.port,
            log_level="info",
            h11_max_incomplete_event_size=None,  # no request size cap (default 16KB header limit only)
            timeout_keep_alive=600,              # keep connection alive for large uploads
        )
        server = uvicorn.Server(config)

        try:
            await server.serve()
        finally:
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

            if self._http_session and not self._http_session.closed:
                await self._http_session.close()

    def _calculate_total_capacity(self) -> float:
        """Calculate total cluster capacity in GB"""
        total = self.config.storage_limit_gb

        for node_info in self.peer_nodes.values():
            total += node_info.get('storage_gb', 0)

        return round(total, 2)

    def _calculate_used_capacity(self) -> float:
        """Calculate used capacity in GB by summing chunk sizes for this node."""
        db = self.SessionLocal()
        try:
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
        """Periodically update registry with latest cluster stats (leader only)"""
        while True:
            try:
                await asyncio.sleep(30)

                if self.registry_client and self.is_leader:
                    node_count = len(self.peer_nodes) + 1
                    total_capacity_gb = self._calculate_total_capacity()
                    used_capacity_gb = self._calculate_used_capacity()

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
    """
    Main entry point — config priority:
      1. Environment variables  (TOSSIT_NODE_NAME + TOSSIT_CLUSTER_ID required)
      2. Persisted YAML config  (~/.tossit/node_config.yaml or $TOSSIT_DATA_DIR/node_config.yaml)
      3. Interactive setup wizard
    """
    # Priority 1: env vars (Docker / CI / scripted deployments)
    config = NodeConfig.from_env()

    if config is None:
        # Priority 2: persisted YAML
        config = NodeConfig.load()

        if not config.exists():
            # Priority 3: interactive wizard (dev / first-run on bare metal)
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
        print("\n\nTossIt node stopped")
        sys.exit(0)
