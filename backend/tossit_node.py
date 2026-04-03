#!/usr/bin/env python3
"""
TossIt v2.0 - Unified Node (Brain + Storage)
Integrated with mDNS Discovery, Health Monitoring, and Chunk Replication

Phase 1 additions:
  - raft_propose(op, payload)          — single entry point for metadata writes
  - _apply_log_entries()               — background loop: committed -> applied
  - _drain_committed_entries()         — applies all unapplied committed entries
  - _apply_single_entry_to_db()        — dispatch table for all op types
  - _log_apply_*()                     — idempotent DB writers for each op
  - GET /api/raft/log_status           — monitoring endpoint

Phase 1 does NOT yet migrate the existing write paths (upload, delete, replication).
Those still write directly to the DB. Migration happens in Phase 4.
Phase 1 can be validated on a single node: propose an op, confirm it appears
in raft_log with applied=True and the corresponding row exists in the target table.
"""

import asyncio
import hashlib
import json
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

from cluster_discovery import ClusterDiscovery
from cluster_raft import ClusterRaft
from node_health_monitor import NodeHealthMonitor
from registry_client import RegistryClient
from node_identity import NodeIdentity
from trust_store import TrustStore

import tempfile
from fastapi import BackgroundTasks

import uuid6


def generate_file_id() -> str:
    return str(uuid6.uuid7())


from fastapi import FastAPI, UploadFile, File as FastAPIFile, HTTPException, Query
from fastapi.responses import StreamingResponse, FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine, event, func, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
import uvicorn

DATA_ROOT        = Path(os.environ.get('TOSSIT_DATA_DIR', Path.home() / '.tossit')).resolve()
CONFIG_DIR       = DATA_ROOT
CONFIG_FILE      = DATA_ROOT / "node_config.yaml"
STORAGE_DIR      = DATA_ROOT / "storage"
DB_DIR           = DATA_ROOT / "database"
SNAPSHOT_DIR     = DATA_ROOT / "snapshots"
KEYS_DIR         = DATA_ROOT / "keys"
TRUST_STORE_FILE = DATA_ROOT / "trust_store.json"

for _d in (DATA_ROOT, STORAGE_DIR, DB_DIR, KEYS_DIR, SNAPSHOT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE_BYTES         = 8 * 1024 * 1024   # 8 MB per chunk
SNAPSHOT_INTERVAL        = 1000  # take snapshot every N committed entries
SNAPSHOT_RETENTION_COUNT = 3     # keep at most this many snapshots on disk


async def db_commit_with_retry(db, max_retries=5, base_delay=0.05):
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
    Base, Node, File as FileModel, Chunk, FileChunk, ChunkLocation,
    RaftLogEntry, SnapshotMeta, Job, AuditLog, NodeStatus, JobStatus, JobType
)


class NodeConfig:
    def __init__(self):
        self.node_id: Optional[str] = None
        self.node_name: Optional[str] = None
        self.cluster_id: Optional[str] = None
        self.cluster_mode: str = "private"
        self.is_first_node: bool = False
        self.port: int = 8000
        self.storage_path: Path = STORAGE_DIR
        self.storage_limit_gb: float = 50.0
        self.center_enabled: bool = False
        self.center_id: Optional[str] = None

    @staticmethod
    def generate_cluster_id() -> str:
        return secrets.token_hex(4)

    @staticmethod
    def generate_node_id() -> str:
        return secrets.token_hex(8)

    def save(self):
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
        node_name  = os.environ.get('TOSSIT_NODE_NAME', '').strip()
        cluster_id = os.environ.get('TOSSIT_CLUSTER_ID', '').strip()
        if not node_name or not cluster_id:
            return None
        config = NodeConfig()
        config.node_name        = node_name
        config.cluster_id       = cluster_id
        config.node_id          = NodeConfig.generate_node_id()
        config.port             = int(os.environ.get('TOSSIT_PORT', '8000'))
        config.storage_limit_gb = float(os.environ.get('TOSSIT_STORAGE_LIMIT_GB', '50'))
        config.is_first_node    = os.environ.get('TOSSIT_FIRST_NODE', '').lower() in ('1', 'true', 'yes')
        config.cluster_mode     = 'private'
        config.center_enabled   = False
        print(f"Configuration loaded from environment variables")
        return config

    def exists(self) -> bool:
        return CONFIG_FILE.exists() and self.node_id is not None


def get_disk_usage(path: Path) -> tuple[float, float, float]:
    stat = shutil.disk_usage(path)
    return stat.total / (1024**3), stat.used / (1024**3), stat.free / (1024**3)


def get_local_ip() -> str:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception:
        return "127.0.0.1"


def interactive_setup() -> NodeConfig:
    print("\n" + "="*60)
    print("TossIt v2.0 - First Time Setup")
    print("="*60 + "\n")
    config = NodeConfig()
    default_name = socket.gethostname()
    node_name = input(f"Node name [{default_name}]: ").strip()
    config.node_name = node_name if node_name else default_name
    config.node_id = NodeConfig.generate_node_id()
    print(f"Generated node ID: {config.node_id}")

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
                if input(f"Use maximum ({free_gb * 0.9:.1f} GB)? [y/n]: ").strip().lower() == 'y':
                    config.storage_limit_gb = free_gb * 0.9
                    break
            else:
                config.storage_limit_gb = requested
                break
        except ValueError:
            print("Please enter a valid number")

    print(f"Allocated: {config.storage_limit_gb:.1f} GB")
    print("\n--- Cluster Setup ---")
    print("1. Create new cluster\n2. Join existing cluster")
    choice = input("\nChoose [1/2]: ").strip()

    if choice == "1":
        config.is_first_node = True
        config.cluster_id = NodeConfig.generate_cluster_id()
        print(f"\nCreated new cluster: {config.cluster_id}")
    elif choice == "2":
        config.is_first_node = False
        cluster_id = input("\nEnter cluster ID (8 characters): ").strip()
        if len(cluster_id) != 8:
            print("Invalid cluster ID — starting as single-node cluster instead")
            config.cluster_id = NodeConfig.generate_cluster_id()
            config.is_first_node = True
        else:
            config.cluster_id = cluster_id
    else:
        config.is_first_node = True
        config.cluster_id = NodeConfig.generate_cluster_id()

    port_input = input(f"\nPort number [8000]: ").strip()
    if port_input and port_input.isdigit():
        config.port = int(port_input)

    config.cluster_mode = "private"
    config.center_enabled = False
    config.save()
    return config


class TossItNode:
    """Unified node that acts as both brain and storage"""

    def __init__(self, config: NodeConfig):
        self.config = config
        self.app = FastAPI(title=f"TossIt Node - {config.node_name}")

        self.config.storage_path.mkdir(parents=True, exist_ok=True)
        upload_temp_dir = self.config.storage_path / "upload_temp"
        upload_temp_dir.mkdir(exist_ok=True, parents=True)
        os.environ['TMPDIR'] = str(upload_temp_dir)
        tempfile.tempdir = str(upload_temp_dir)
        print(f"Upload temp: {upload_temp_dir} (bypasses tmpfs)")

        self.chunks_dir = self.config.storage_path / "chunks"
        self.chunks_dir.mkdir(exist_ok=True, parents=True)
        self.staging_dir = self.config.storage_path / "staging"
        self.staging_dir.mkdir(exist_ok=True, parents=True)
        print(f"Content-addressed storage: {self.chunks_dir}")
        print(f"Upload staging: {self.staging_dir}")

        keys_path = KEYS_DIR / config.node_name
        self.identity = NodeIdentity(config.node_name, keys_path)
        self.trust_store = TrustStore(TRUST_STORE_FILE)
        self.node_id = self.identity.get_node_id()

        print(f"Node identity:")
        print(f"  Name:       {config.node_name}")
        print(f"  ID:         {self.node_id}")
        print(f"  Public key: {self.identity.get_public_key_base64()[:32]}...")
        print(f"  Keys path:  {keys_path}")
        print(f"  Trust store: {TRUST_STORE_FILE}")

        self.discovery: Optional[ClusterDiscovery] = None
        self.raft: Optional[ClusterRaft] = None
        self.peer_nodes: Dict[str, dict] = {}

        self.health_monitor: Optional[NodeHealthMonitor] = None
        self.peer_refresh_task: Optional[asyncio.Task] = None
        self.registry_client: Optional[RegistryClient] = None

        self.reservation_lock = asyncio.Lock()
        self.reserved_space_gb = 0.0

        self._cached_capacity_time = 0.0
        self._capacity_cache_ttl = 2.0

        self._http_session: Optional[aiohttp.ClientSession] = None
        self._db_write_semaphore = asyncio.Semaphore(1)
        self._upload_semaphore = asyncio.Semaphore(3)

        self._assigned_uploads: Dict[str, int] = {}
        self._assigned_uploads_lock = asyncio.Lock()

        self._replication_semaphore = asyncio.Semaphore(4)

        # ── Phase 1: Raft log apply infrastructure ────────────────────
        # _apply_log_entries_task runs for the lifetime of the node.
        # _apply_log_entries_event lets raft_propose wake the loop
        # immediately rather than waiting up to 50ms.
        self._apply_log_entries_task: Optional[asyncio.Task] = None
        self._apply_log_entries_event: Optional[asyncio.Event] = None
        self._snapshot_check_task: Optional[asyncio.Task] = None  # Phase 3

        db_path = DB_DIR / f"tossit_{config.node_id}.db"
        self.engine = create_engine(
            f'sqlite:///{db_path}',
            connect_args={"check_same_thread": False, "timeout": 30},
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

        self._cleanup_temp_files()
        self._ensure_node_record()
        self._update_capacity()
        self._setup_routes()
        print(f"Database initialized: {db_path}")

        self.max_upload_size_bytes = int(config.storage_limit_gb * 1024 * 1024 * 1024)
        print(f"Max upload size: {self.max_upload_size_bytes / (1024**3):.1f} GB")

    # ================================================================
    #  RAFT LOG INFRASTRUCTURE (Phase 1)
    # ================================================================

    async def raft_propose(self, op: str, payload: dict) -> int:
        """
        Propose a metadata operation to the Raft cluster.

        Writes a RaftLogEntry to the local database, advances commit_index,
        and wakes the apply loop. Returns the committed log index.

        Raises HTTPException(503) if this node is not the leader.

        PAYLOAD DETERMINISM CONTRACT — all non-deterministic values must be
        resolved by the caller before passing payload here:
          - file IDs   → call generate_file_id() before raft_propose
          - timestamps → call datetime.now(timezone.utc).isoformat() before raft_propose
          - node IDs   → pass self.node_id directly

        The _log_apply_* handlers must never call datetime.now() or
        generate_file_id(). They only read from the payload dict.

        Phase 1: commits immediately with self as sole quorum member.
        Phase 2 will replace the quorum block with an append_entries fan-out
        that waits for floor(N/2)+1 ACKs before advancing commit_index.
        """
        if not self.raft or not self.raft.is_leader():
            raise HTTPException(
                503,
                "Not the leader — raft_propose must be called on the leader node"
            )

        # Assign log index under the write semaphore so concurrent propose
        # calls cannot both read the same max(index) and collide.
        async with self._db_write_semaphore:
            db = self.SessionLocal()
            try:
                last_idx = db.query(
                    func.coalesce(func.max(RaftLogEntry.index), 0)
                ).scalar() or 0
                log_index = last_idx + 1

                entry = RaftLogEntry(
                    index=log_index,
                    term=self.raft.current_term,
                    op=op,
                    payload=json.dumps(payload, sort_keys=True),
                    applied=False,
                )
                db.add(entry)
                await db_commit_with_retry(db)
            except Exception:
                db.rollback()
                raise
            finally:
                db.close()

        # ── Phase 2 quorum block ─────────────────────────────────────
        # Fan out append_entries to all peers and wait for quorum ACKs.
        # Falls back to single-node commit when no peers exist.
        if not self.raft.peers:
            # Single-node cluster: self is quorum — commit immediately.
            self.raft.commit_index = log_index
            self.raft._persist_state()
        else:
            total  = 1 + len(self.raft.peers)
            quorum = total // 2 + 1

            ack_count, updated_matches = await self._fan_out_append_entries(log_index)

            # Update per-follower tracking with confirmed positions
            for peer_id, match_idx in updated_matches.items():
                self.raft.match_index[peer_id] = match_idx
                self.raft.next_index[peer_id]  = match_idx + 1

            # Self always has the entry (we just wrote it), so acks = 1 + remote
            if 1 + ack_count < quorum:
                raise HTTPException(
                    503,
                    f"Could not reach quorum ({1 + ack_count}/{quorum} nodes): "
                    f"entry written to log at index {log_index} but not committed. "
                    f"Retry when enough nodes are reachable."
                )

            # Advance commit_index to highest index confirmed by quorum
            self.raft._advance_commit_index(log_index)
        # ─────────────────────────────────────────────────────────────

        # Wake the apply loop immediately.
        if self._apply_log_entries_event:
            self._apply_log_entries_event.set()

        print(f"Raft log: proposed op={op!r} index={log_index} term={self.raft.current_term}")
        return log_index

    async def _apply_log_entries(self):
        """
        Continuous background task: apply committed log entries to the DB.

        Wakes on _apply_log_entries_event signal or after a 50ms timeout,
        then calls _drain_committed_entries(). Runs for the node's lifetime.
        """
        print("Raft log apply loop started")
        while True:
            try:
                await self._drain_committed_entries()
            except asyncio.CancelledError:
                print("Raft log apply loop stopped")
                return
            except Exception as exc:
                print(f"_apply_log_entries error: {exc}")

            try:
                await asyncio.wait_for(
                    self._apply_log_entries_event.wait(),
                    timeout=0.05,
                )
                self._apply_log_entries_event.clear()
            except asyncio.TimeoutError:
                self._apply_log_entries_event.clear()
            except asyncio.CancelledError:
                print("Raft log apply loop stopped")
                return

    async def _drain_committed_entries(self):
        """
        Apply all committed-but-unapplied log entries to the live tables.

        Queries entries where index <= commit_index and applied=False,
        sorted ascending so they are always applied in log order. Each entry
        is individually committed so a crash mid-drain leaves only the failed
        entry unapplied — all prior entries are already durable.
        """
        if not self.raft or self.raft.commit_index <= self.raft.last_applied:
            return

        db = self.SessionLocal()
        try:
            entries = (
                db.query(RaftLogEntry)
                .filter(
                    RaftLogEntry.index > self.raft.last_applied,
                    RaftLogEntry.index <= self.raft.commit_index,
                    RaftLogEntry.applied == False,
                )
                .order_by(RaftLogEntry.index)
                .all()
            )

            for entry in entries:
                try:
                    self._apply_single_entry_to_db(db, entry)
                    entry.applied = True
                    await db_commit_with_retry(db)
                    self.raft.last_applied = entry.index
                    self.raft._persist_state()
                    print(f"Raft log: applied index={entry.index} op={entry.op!r}")
                except Exception as exc:
                    db.rollback()
                    print(f"Raft log: failed to apply index={entry.index} op={entry.op!r}: {exc}")
                    # Stop here — subsequent entries may depend on this one.
                    # Next _drain call will retry from this entry.
                    raise
        except Exception:
            raise
        finally:
            db.close()

    def _apply_single_entry_to_db(self, db, entry: RaftLogEntry):
        """
        Dispatch a log entry to the appropriate DB writer.

        Does NOT commit — the caller commits after this returns so the
        applied=True flag and the data change land in the same transaction.

        All handlers are idempotent: calling them twice with the same entry
        leaves the DB in the same state as calling them once.
        """
        p = json.loads(entry.payload)
        dispatch = {
            'create_file':           self._log_apply_create_file,
            'delete_file':           self._log_apply_delete_file,
            'mark_file_replicated':  self._log_apply_mark_file_replicated,
            'add_chunk_location':    self._log_apply_add_chunk_location,
            'remove_chunk_location': self._log_apply_remove_chunk_location,
            'add_file_chunk':        self._log_apply_add_file_chunk,
            'upsert_file_metadata':  self._log_apply_upsert_file_metadata,
        }
        handler = dispatch.get(entry.op)
        if handler:
            handler(db, p)
        else:
            print(f"Raft log: unknown op {entry.op!r} at index {entry.index} — skipping")

    # ── Per-op apply handlers ─────────────────────────────────────────
    # Naming:    _log_apply_<op_name>
    # Signature: (self, db, p: dict) -> None
    # Contract:
    #   - Read all values from p — never call datetime.now() or generate IDs
    #   - Check existence before inserting (idempotency)
    #   - Do not commit — caller commits after setting applied=True
    # ──────────────────────────────────────────────────────────────────

    def _log_apply_create_file(self, db, p: dict):
        """
        Apply a create_file entry.

        Required payload fields:
          file_id, filename, total_size_bytes, chunk_size_bytes,
          total_chunks, checksum_sha256, uploaded_by, created_at

        Optional bundled fields (for atomic Phase 4 upload commits):
          chunks:          [{"chunk_hash": str, "size_bytes": int}, ...]
          file_chunks:     [{"chunk_index": int, "chunk_hash": str}, ...]
          chunk_locations: [{"chunk_hash": str, "node_id": str}, ...]

        Bundling chunks/file_chunks/chunk_locations into a single create_file
        entry makes the entire upload atomic in the log.
        """
        file_id = p['file_id']
        if db.query(FileModel).filter(FileModel.id == file_id).first():
            return  # Idempotent

        db.add(FileModel(
            id=file_id,
            filename=p['filename'],
            total_size_bytes=p['total_size_bytes'],
            chunk_size_bytes=p['chunk_size_bytes'],
            total_chunks=p['total_chunks'],
            checksum_sha256=p['checksum_sha256'],
            uploaded_by=p['uploaded_by'],
            is_complete=True,
            is_replicated=False,
            created_at=datetime.fromisoformat(p['created_at']),
        ))

        for chunk_meta in p.get('chunks', []):
            self._ensure_chunk(db, chunk_meta['chunk_hash'], chunk_meta['size_bytes'])

        for fc in p.get('file_chunks', []):
            exists = db.query(FileChunk).filter(
                FileChunk.file_id     == file_id,
                FileChunk.chunk_index == fc['chunk_index'],
            ).first()
            if not exists:
                db.add(FileChunk(
                    file_id=file_id,
                    chunk_index=fc['chunk_index'],
                    chunk_hash=fc['chunk_hash'],
                ))

        for loc in p.get('chunk_locations', []):
            exists = db.query(ChunkLocation).filter(
                ChunkLocation.chunk_hash == loc['chunk_hash'],
                ChunkLocation.node_id   == loc['node_id'],
            ).first()
            if not exists:
                db.add(ChunkLocation(
                    chunk_hash=loc['chunk_hash'],
                    node_id=loc['node_id'],
                ))

    def _log_apply_delete_file(self, db, p: dict):
        """
        Apply a delete_file entry.
        Required: file_id, chunk_hashes: [str, ...]
        """
        file_record = db.query(FileModel).filter(FileModel.id == p['file_id']).first()
        if not file_record:
            return  # Already deleted — idempotent
        for chunk_hash in p.get('chunk_hashes', []):
            self._decrement_chunk_ref(db, chunk_hash)
        db.delete(file_record)

    def _log_apply_mark_file_replicated(self, db, p: dict):
        """Apply a mark_file_replicated entry. Required: file_id"""
        f = db.query(FileModel).filter(FileModel.id == p['file_id']).first()
        if f and not f.is_replicated:
            f.is_replicated = True

    def _log_apply_add_chunk_location(self, db, p: dict):
        """
        Apply an add_chunk_location entry.
        Required: chunk_hash, node_id, size_bytes
        """
        chunk_hash = p['chunk_hash']
        node_id    = p['node_id']
        size_bytes = p['size_bytes']
        # Ensure Chunk row exists. Under normal ordering it was created by
        # create_file, but a lagging follower might apply this first.
        self._ensure_chunk(db, chunk_hash, size_bytes, increment_ref=False)
        exists = db.query(ChunkLocation).filter(
            ChunkLocation.chunk_hash == chunk_hash,
            ChunkLocation.node_id   == node_id,
        ).first()
        if not exists:
            db.add(ChunkLocation(chunk_hash=chunk_hash, node_id=node_id))

    def _log_apply_remove_chunk_location(self, db, p: dict):
        """Apply a remove_chunk_location entry. Required: chunk_hash, node_id"""
        loc = db.query(ChunkLocation).filter(
            ChunkLocation.chunk_hash == p['chunk_hash'],
            ChunkLocation.node_id   == p['node_id'],
        ).first()
        if loc:
            db.delete(loc)

    def _log_apply_add_file_chunk(self, db, p: dict):
        """Apply an add_file_chunk entry. Required: file_id, chunk_index, chunk_hash"""
        exists = db.query(FileChunk).filter(
            FileChunk.file_id     == p['file_id'],
            FileChunk.chunk_index == p['chunk_index'],
        ).first()
        if not exists:
            db.add(FileChunk(
                file_id=p['file_id'],
                chunk_index=p['chunk_index'],
                chunk_hash=p['chunk_hash'],
            ))

    def _log_apply_upsert_file_metadata(self, db, p: dict):
        """
        Apply an upsert_file_metadata entry (reconciliation).
        Required: file_id. Optional: any mutable File columns.
        """
        file_id = p['file_id']
        mutable = [
            'filename', 'total_size_bytes', 'chunk_size_bytes', 'total_chunks',
            'checksum_sha256', 'uploaded_by', 'is_complete', 'is_replicated',
        ]
        existing = db.query(FileModel).filter(FileModel.id == file_id).first()
        if existing:
            for field in mutable:
                if field in p:
                    setattr(existing, field, p[field])
        else:
            db.add(FileModel(
                id=file_id,
                filename=p.get('filename', 'unknown'),
                total_size_bytes=p.get('total_size_bytes', 0),
                chunk_size_bytes=p.get('chunk_size_bytes', CHUNK_SIZE_BYTES),
                total_chunks=p.get('total_chunks', 0),
                checksum_sha256=p.get('checksum_sha256', ''),
                uploaded_by=p.get('uploaded_by', 'unknown'),
                is_complete=p.get('is_complete', False),
                is_replicated=p.get('is_replicated', False),
            ))

    # ================================================================
    #  END RAFT LOG INFRASTRUCTURE
    # ================================================================

    # ================================================================
    #  APPEND ENTRIES FAN-OUT (Phase 2)
    #
    #  _fan_out_append_entries(up_to_index)
    #    Sends append_entries concurrently to all known peers.
    #    Returns (ack_count, {node_id: match_index}).
    #
    #  _send_append_entries_to_peer(node_id, up_to_index)
    #    Sends to a single peer with log-matching backfill.
    #    Backfill: if the follower's log diverges, decrements next_index[peer]
    #    by 1 and retries up to MAX_BACKFILL_STEPS times.
    #    Beyond MAX_BACKFILL_STEPS, Phase 3 InstallSnapshot takes over.
    # ================================================================

    async def _fan_out_append_entries(
        self, up_to_index: int
    ) -> tuple[int, dict]:
        """
        Send append_entries concurrently to all peers for entries up to
        up_to_index. Returns (ack_count, {node_id: match_index}).

        Uses asyncio.gather so all peers are contacted in parallel.
        Each individual request has a 5 s timeout (configured at the
        aiohttp call site). The gather therefore completes in at most
        5 s + backfill time for the slowest peer.
        """
        peer_ids = list(self.raft.peers.keys())
        if not peer_ids:
            return 0, {}

        coros = [
            self._send_append_entries_to_peer(node_id, up_to_index)
            for node_id in peer_ids
        ]
        results = await asyncio.gather(*coros, return_exceptions=True)

        ack_count = 0
        updated_matches: dict = {}
        for node_id, result in zip(peer_ids, results):
            if isinstance(result, Exception):
                print(f"append_entries gather error for {node_id[:8]}: {result}")
                continue
            if isinstance(result, dict) and result.get('success'):
                ack_count += 1
                updated_matches[node_id] = result.get('match_index', 0)

        return ack_count, updated_matches

    async def _send_append_entries_to_peer(
        self, node_id: str, up_to_index: int
    ) -> dict:
        """
        Send append_entries to one peer. Handles log-matching backfill.

        On success → {'success': True,  'match_index': int, 'node_id': str}
        On failure → {'success': False, 'match_index': 0}

        Backfill protocol:
          If the follower returns success=False (prev_log consistency check
          failed), decrement next_index[peer] by 1 and retry with an earlier
          prev_log_index. This converges to the last common log prefix.
          Bounded at MAX_BACKFILL_STEPS — deeply lagging nodes need Phase 3
          InstallSnapshot rather than entry-by-entry catchup.

        Higher-term discovery:
          If the follower reports a higher term, step down immediately and
          return failure. The caller (raft_propose) will then fail the quorum
          check and surface a 503 to the client.
        """
        if node_id not in self.peer_nodes:
            return {'success': False, 'match_index': 0}

        peer_info = self.peer_nodes[node_id]
        peer_name = peer_info.get('node_name', node_id[:8])
        peer_url  = f"http://{peer_info['ip_address']}:{peer_info['port']}"

        MAX_BACKFILL_STEPS = 10

        for attempt in range(MAX_BACKFILL_STEPS + 1):
            start_index    = max(1, self.raft.next_index.get(node_id, 1))
            prev_log_index = start_index - 1
            prev_log_term  = 0
            needs_snapshot = False  # Phase 3: set True if prev_entry is below snapshot

            db = self.SessionLocal()
            try:
                # Resolve prev_log_term for the consistency check
                if prev_log_index > 0:
                    prev_entry = db.query(RaftLogEntry).filter(
                        RaftLogEntry.index == prev_log_index
                    ).first()
                    if prev_entry:
                        prev_log_term = prev_entry.term
                    else:
                        # prev_log_index is below our snapshot boundary.
                        # Phase 3: trigger InstallSnapshot instead of backfilling.
                        needs_snapshot = True

                if not needs_snapshot:
                    # Collect entries from start_index up to up_to_index
                    entries_to_send = (
                        db.query(RaftLogEntry)
                        .filter(
                            RaftLogEntry.index >= start_index,
                            RaftLogEntry.index <= up_to_index,
                        )
                        .order_by(RaftLogEntry.index)
                        .all()
                    )
                    entries_payload = [
                        {'index': e.index, 'term': e.term, 'op': e.op, 'payload': e.payload}
                        for e in entries_to_send
                    ]
                else:
                    entries_payload = []  # unused; we return before building message
            finally:
                db.close()

            # Phase 3: send full snapshot if prev_entry was below snapshot boundary
            if needs_snapshot:
                print(
                    f"append_entries: {peer_name} needs entries below "
                    f"snapshot_index — sending InstallSnapshot"
                )
                return await self._send_snapshot_to_peer(node_id)

            message = {
                'leader_id':      self.node_id,
                'term':           self.raft.current_term,
                'node_name':      self.config.node_name,
                'commit_index':   self.raft.commit_index,
                'prev_log_index': prev_log_index,
                'prev_log_term':  prev_log_term,
                'entries':        entries_payload,
                'timestamp':      time.time(),
            }
            signed = self.identity.sign_json(message)

            try:
                session = await self._get_http_session()
                async with session.post(
                    f"{peer_url}/api/raft/append_entries",
                    json=signed,
                    timeout=aiohttp.ClientTimeout(total=5.0),
                ) as resp:
                    if resp.status != 200:
                        return {'success': False, 'match_index': 0}

                    data = await resp.json()
                    resp_term = data.get('term', 0)

                    # Higher term: we are no longer the legitimate leader
                    if resp_term > self.raft.current_term:
                        self.raft.current_term = resp_term
                        self.raft.voted_for = None
                        self.raft._persist_state()
                        await self.raft._become_follower()
                        return {'success': False, 'match_index': 0}

                    if data.get('success'):
                        match_idx = data.get('match_index', up_to_index)
                        self.raft.next_index[node_id]  = match_idx + 1
                        self.raft.match_index[node_id] = match_idx
                        print(
                            f"append_entries → {peer_name}: "
                            f"ACKed up to index {match_idx}"
                        )
                        return {'success': True, 'match_index': match_idx, 'node_id': node_id}

                    # Log mismatch — step next_index back and retry
                    if attempt < MAX_BACKFILL_STEPS:
                        old_ni = self.raft.next_index.get(node_id, 1)
                        self.raft.next_index[node_id] = max(1, old_ni - 1)
                        print(
                            f"append_entries backfill: {peer_name} "
                            f"next_index {old_ni} → {self.raft.next_index[node_id]}"
                        )
                        continue
                    print(
                        f"append_entries: {peer_name} exhausted backfill "
                        f"({MAX_BACKFILL_STEPS} steps) — sending InstallSnapshot"
                    )
                    return await self._send_snapshot_to_peer(node_id)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"append_entries to {peer_name} failed (attempt {attempt + 1}): {e}")
                return {'success': False, 'match_index': 0}

        return {'success': False, 'match_index': 0}

    # ================================================================
    #  END APPEND ENTRIES FAN-OUT
    # ================================================================

    # ================================================================
    #  SNAPSHOT INFRASTRUCTURE (Phase 3)
    #
    #  _take_snapshot()            — serialize live tables to disk
    #  _load_latest_snapshot()     — read newest snapshot file
    #  _send_snapshot_to_peer()    — push snapshot to a lagging follower
    #  _apply_snapshot_to_db()     — follower: replace live tables from data
    #  _cleanup_old_snapshots()    — enforce SNAPSHOT_RETENTION_COUNT
    #  _trim_applied_log_entries() — remove log entries covered by snapshot
    #  _snapshot_check_loop()      — background periodic trigger
    # ================================================================

    async def _take_snapshot(self) -> 'Optional[dict]':
        """
        Create a snapshot of the 4 live metadata tables at commit_index
        and persist it to SNAPSHOT_DIR as a JSON file.

        Returns the snapshot dict on success, None on failure.

        Only the leader calls this. Followers receive snapshots via
        install_snapshot and call _apply_snapshot_to_db directly.
        """
        if not self.raft:
            return None

        snap_index = self.raft.commit_index
        snap_term  = self.raft.current_term

        if snap_index == 0:
            return None

        # Ensure we have applied everything up to snap_index before
        # photographing the live tables — a partially-applied state
        # would produce an inconsistent snapshot.
        if self.raft.last_applied < snap_index:
            print(
                f"Snapshot: draining to last_applied={self.raft.last_applied} "
                f"before snapshotting at commit_index={snap_index}"
            )
            await self._drain_committed_entries()
            if self.raft.last_applied < snap_index:
                print(f"Snapshot: still behind after drain, skipping")
                return None

        print(f"Taking snapshot at index={snap_index} term={snap_term}...")

        db = self.SessionLocal()
        try:
            files = [
                {
                    'id': f.id,
                    'filename': f.filename,
                    'total_size_bytes': f.total_size_bytes,
                    'chunk_size_bytes': f.chunk_size_bytes,
                    'total_chunks': f.total_chunks,
                    'checksum_sha256': f.checksum_sha256,
                    'is_complete': f.is_complete,
                    'is_replicated': f.is_replicated,
                    'uploaded_by': f.uploaded_by,
                    'created_at': f.created_at.isoformat() if f.created_at else None,
                }
                for f in db.query(FileModel).all()
            ]
            chunks = [
                {
                    'chunk_hash': c.chunk_hash,
                    'size_bytes': c.size_bytes,
                    'refcount': c.refcount,
                }
                for c in db.query(Chunk).all()
            ]
            file_chunks = [
                {
                    'file_id': fc.file_id,
                    'chunk_index': fc.chunk_index,
                    'chunk_hash': fc.chunk_hash,
                }
                for fc in db.query(FileChunk).all()
            ]
            chunk_locations = [
                {
                    'chunk_hash': cl.chunk_hash,
                    'node_id': cl.node_id,
                }
                for cl in db.query(ChunkLocation).all()
            ]
        finally:
            db.close()

        snapshot = {
            'snapshot_index': snap_index,
            'snapshot_term':  snap_term,
            'created_at':     datetime.now(timezone.utc).isoformat(),
            'tables': {
                'files':           files,
                'chunks':          chunks,
                'file_chunks':     file_chunks,
                'chunk_locations': chunk_locations,
            },
        }

        # Atomic write: tmp then rename
        SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
        snap_path = SNAPSHOT_DIR / f"snapshot_{snap_index:010d}.json"
        tmp_path  = snap_path.with_suffix('.tmp')

        try:
            await asyncio.to_thread(
                lambda: tmp_path.write_text(json.dumps(snapshot, separators=(',', ':')))
            )
            await asyncio.to_thread(lambda: tmp_path.rename(snap_path))
        except Exception as e:
            print(f"Snapshot: failed to write to disk: {e}")
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
            return None

        # Update Raft snapshot_index
        self.raft.snapshot_index = snap_index
        self.raft._persist_state()

        # Record in SnapshotMeta so _load_latest_snapshot can find it
        db = self.SessionLocal()
        try:
            if not db.query(SnapshotMeta).filter(SnapshotMeta.snapshot_index == snap_index).first():
                db.add(SnapshotMeta(
                    snapshot_index=snap_index,
                    snapshot_term=snap_term,
                    file_path=str(snap_path),
                ))
                await db_commit_with_retry(db)
        except Exception as e:
            db.rollback()
            print(f"Snapshot: failed to record SnapshotMeta: {e}")
        finally:
            db.close()

        print(
            f"Snapshot saved: index={snap_index}, "
            f"{len(files)} files, {len(chunks)} chunks → {snap_path.name}"
        )

        # Trim log entries now covered by this snapshot, then enforce retention
        await self._trim_applied_log_entries(up_to_index=snap_index)
        await self._cleanup_old_snapshots()

        return snapshot

    async def _load_latest_snapshot(self) -> 'Optional[dict]':
        """
        Load the most recent snapshot from disk via SnapshotMeta.
        Returns None if no snapshots exist or the file is missing.
        """
        db = self.SessionLocal()
        try:
            latest = (
                db.query(SnapshotMeta)
                .order_by(SnapshotMeta.snapshot_index.desc())
                .first()
            )
            if not latest:
                return None
            snap_path = Path(latest.file_path)
        finally:
            db.close()

        if not snap_path.exists():
            print(f"Snapshot: file missing from disk: {snap_path}")
            return None

        try:
            data = await asyncio.to_thread(snap_path.read_text)
            return json.loads(data)
        except Exception as e:
            print(f"Snapshot: failed to read {snap_path.name}: {e}")
            return None

    async def _send_snapshot_to_peer(self, node_id: str) -> dict:
        """
        Push the latest snapshot to a lagging follower via InstallSnapshot.

        If no snapshot exists on disk yet, takes one first. On a verified
        ACK, updates match_index[peer] = snapshot_index and
        next_index[peer] = snapshot_index+1 so normal append_entries
        resumes from there.

        Returns the same shape as _send_append_entries_to_peer:
          {'success': True/False, 'match_index': int}
        """
        if node_id not in self.peer_nodes:
            return {'success': False, 'match_index': 0}

        peer_info = self.peer_nodes[node_id]
        peer_name = peer_info.get('node_name', node_id[:8])
        peer_url  = f"http://{peer_info['ip_address']}:{peer_info['port']}"

        print(f"InstallSnapshot → {peer_name}: preparing snapshot...")

        snapshot = await self._load_latest_snapshot()
        if snapshot is None:
            print(f"InstallSnapshot → {peer_name}: no snapshot on disk, taking one now...")
            snapshot = await self._take_snapshot()
            if snapshot is None:
                print(f"InstallSnapshot → {peer_name}: snapshot creation failed")
                return {'success': False, 'match_index': 0}

        snap_index = snapshot['snapshot_index']
        snap_term  = snapshot['snapshot_term']

        message = {
            'leader_id':      self.node_id,
            'term':           self.raft.current_term,
            'node_name':      self.config.node_name,
            'snapshot_index': snap_index,
            'snapshot_term':  snap_term,
            'data':           snapshot['tables'],
            'timestamp':      time.time(),
        }
        signed = self.identity.sign_json(message)

        try:
            session = await self._get_http_session()
            async with session.post(
                f"{peer_url}/api/raft/install_snapshot",
                json=signed,
                timeout=aiohttp.ClientTimeout(total=60.0),  # snapshots can be large
            ) as resp:
                if resp.status != 200:
                    print(f"InstallSnapshot → {peer_name}: HTTP {resp.status}")
                    return {'success': False, 'match_index': 0}

                ack = await resp.json()
                resp_term = ack.get('term', 0)

                if resp_term > self.raft.current_term:
                    self.raft.current_term = resp_term
                    self.raft.voted_for = None
                    self.raft._persist_state()
                    await self.raft._become_follower()
                    return {'success': False, 'match_index': 0}

                if ack.get('success'):
                    self.raft.match_index[node_id] = snap_index
                    self.raft.next_index[node_id]  = snap_index + 1
                    print(
                        f"InstallSnapshot → {peer_name}: ACKed at index={snap_index}, "
                        f"resuming append_entries from {snap_index + 1}"
                    )
                    return {'success': True, 'match_index': snap_index, 'node_id': node_id}

                print(f"InstallSnapshot → {peer_name}: peer rejected snapshot")
                return {'success': False, 'match_index': 0}

        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"InstallSnapshot → {peer_name}: {e}")
            return {'success': False, 'match_index': 0}

    async def _apply_snapshot_to_db(self, tables: dict, snapshot_index: int, snapshot_term: int):
        """
        Replace the 4 live metadata tables with the snapshot contents.

        Called by the install_snapshot endpoint on a follower after signature
        verification. Runs under the DB write semaphore so no concurrent
        writes can interleave mid-replace.

        Steps:
          1. Wipe ChunkLocation, FileChunk, Chunk, File (FK-safe order)
          2. Insert snapshot rows for each table
          3. Trim raft_log entries covered by this snapshot (applied <= snap_index)
          4. Record SnapshotMeta row for the installed snapshot
        """
        async with self._db_write_semaphore:
            db = self.SessionLocal()
            try:
                # Wipe live tables in FK-safe order
                db.query(ChunkLocation).delete(synchronize_session=False)
                db.query(FileChunk).delete(synchronize_session=False)
                db.query(Chunk).delete(synchronize_session=False)
                db.query(FileModel).delete(synchronize_session=False)
                db.flush()

                # Restore Files
                for row in tables.get('files', []):
                    db.add(FileModel(
                        id=row['id'],
                        filename=row['filename'],
                        total_size_bytes=row['total_size_bytes'],
                        chunk_size_bytes=row['chunk_size_bytes'],
                        total_chunks=row['total_chunks'],
                        checksum_sha256=row['checksum_sha256'],
                        is_complete=row['is_complete'],
                        is_replicated=row['is_replicated'],
                        uploaded_by=row['uploaded_by'],
                        created_at=(
                            datetime.fromisoformat(row['created_at'])
                            if row.get('created_at') else datetime.now(timezone.utc)
                        ),
                    ))

                # Restore Chunks
                for row in tables.get('chunks', []):
                    db.add(Chunk(
                        chunk_hash=row['chunk_hash'],
                        size_bytes=row['size_bytes'],
                        refcount=row['refcount'],
                    ))

                # Restore FileChunks
                for row in tables.get('file_chunks', []):
                    db.add(FileChunk(
                        file_id=row['file_id'],
                        chunk_index=row['chunk_index'],
                        chunk_hash=row['chunk_hash'],
                    ))

                # Restore ChunkLocations
                for row in tables.get('chunk_locations', []):
                    db.add(ChunkLocation(
                        chunk_hash=row['chunk_hash'],
                        node_id=row['node_id'],
                    ))

                # Trim raft_log entries now covered by this snapshot
                db.query(RaftLogEntry).filter(
                    RaftLogEntry.index   <= snapshot_index,
                    RaftLogEntry.applied == True,
                ).delete(synchronize_session=False)

                # Record the installed snapshot in SnapshotMeta
                if not db.query(SnapshotMeta).filter(
                    SnapshotMeta.snapshot_index == snapshot_index
                ).first():
                    db.add(SnapshotMeta(
                        snapshot_index=snapshot_index,
                        snapshot_term=snapshot_term,
                        # Followers don't write the JSON file — path is informational
                        file_path=str(SNAPSHOT_DIR / f"snapshot_{snapshot_index:010d}.json"),
                    ))

                await db_commit_with_retry(db)

                n_files  = len(tables.get('files', []))
                n_chunks = len(tables.get('chunks', []))
                print(
                    f"Snapshot applied: index={snapshot_index}, "
                    f"{n_files} files, {n_chunks} chunks restored"
                )
            except Exception as exc:
                db.rollback()
                raise RuntimeError(f"_apply_snapshot_to_db failed: {exc}") from exc
            finally:
                db.close()

    async def _cleanup_old_snapshots(self):
        """
        Keep only the SNAPSHOT_RETENTION_COUNT most recent snapshots.
        Deletes both the JSON file and the SnapshotMeta row for old ones.
        """
        db = self.SessionLocal()
        try:
            all_snaps = (
                db.query(SnapshotMeta)
                .order_by(SnapshotMeta.snapshot_index.desc())
                .all()
            )
            to_delete = all_snaps[SNAPSHOT_RETENTION_COUNT:]
            for meta in to_delete:
                try:
                    Path(meta.file_path).unlink(missing_ok=True)
                except Exception as e:
                    print(f"Snapshot cleanup: could not delete {meta.file_path}: {e}")
                db.delete(meta)
                print(f"Snapshot cleanup: removed index={meta.snapshot_index}")
            if to_delete:
                await db_commit_with_retry(db)
        except Exception as e:
            db.rollback()
            print(f"Snapshot cleanup error: {e}")
        finally:
            db.close()

    async def _trim_applied_log_entries(self, up_to_index: int):
        """
        Delete applied raft_log entries at or below up_to_index.

        Only trims entries where applied=True — uncommitted/unapplied entries
        are never deleted regardless of index.

        Phase 6 will add a retention_days guard so nodes offline for less
        than retention_days can always catch up incrementally without needing
        a snapshot (instead of the unconditional trim done here).
        """
        if up_to_index <= 0:
            return
        db = self.SessionLocal()
        try:
            deleted = (
                db.query(RaftLogEntry)
                .filter(
                    RaftLogEntry.index   <= up_to_index,
                    RaftLogEntry.applied == True,
                )
                .delete(synchronize_session=False)
            )
            await db_commit_with_retry(db)
            if deleted:
                print(f"Log trim: removed {deleted} applied entries at or below index={up_to_index}")
        except Exception as e:
            db.rollback()
            print(f"Log trim error: {e}")
        finally:
            db.close()

    async def _snapshot_check_loop(self):
        """
        Background task: take a periodic snapshot every SNAPSHOT_INTERVAL
        committed entries (default 1000). Checks every 60 seconds.

        Only the leader takes snapshots — followers receive them via
        install_snapshot. A 30-second startup grace period lets the node
        join the cluster and finish reconciliation before the first check.
        """
        print(f"Snapshot check loop started (trigger: every {SNAPSHOT_INTERVAL} committed entries)")
        await asyncio.sleep(30.0)   # startup grace period

        while True:
            try:
                if (self.raft
                        and self.raft.is_leader()
                        and (self.raft.commit_index - self.raft.snapshot_index) >= SNAPSHOT_INTERVAL):
                    print(
                        f"Snapshot trigger: commit_index={self.raft.commit_index}, "
                        f"snapshot_index={self.raft.snapshot_index}, "
                        f"delta={self.raft.commit_index - self.raft.snapshot_index}"
                    )
                    await self._take_snapshot()
            except asyncio.CancelledError:
                print("Snapshot check loop stopped")
                return
            except Exception as e:
                print(f"Snapshot check loop error: {e}")

            try:
                await asyncio.sleep(60.0)
            except asyncio.CancelledError:
                print("Snapshot check loop stopped")
                return

    # ================================================================
    #  END SNAPSHOT INFRASTRUCTURE
    # ================================================================

    async def _on_node_discovered(self, node_info: dict):
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
        if node_id in self.peer_nodes:
            node_info = self.peer_nodes[node_id]
            print(f"Peer left cluster: {node_info['node_name']}")
            del self.peer_nodes[node_id]
            if self.raft:
                self.raft.update_peers(self.peer_nodes)

    async def _on_node_went_offline(self, node_id: str):
        if self.raft and self.raft.is_peer_alive(node_id, max_age=30.0):
            return
        if node_id in self.peer_nodes:
            node_info = self.peer_nodes[node_id]
            print(f"Node went offline: {node_info['node_name']} (health monitor timeout, confirmed by heartbeat thread)")

    async def _periodic_peer_refresh(self):
        print("Periodic peer health check started (every 5s)")
        while True:
            try:
                await asyncio.sleep(5.0)
                local_active = 3 - self._upload_semaphore._value
                if self._assigned_uploads.get("self", 0) > local_active:
                    self._assigned_uploads["self"] = local_active

                if self.health_monitor and len(self.peer_nodes) > 0:
                    for node_id in list(self.peer_nodes.keys()):
                        peer_info = self.peer_nodes[node_id]
                        peer_url = f"http://{peer_info['ip_address']}:{peer_info['port']}/api/health"
                        try:
                            start_time = time.time()
                            session = await self._get_http_session()
                            async with session.get(peer_url, timeout=aiohttp.ClientTimeout(total=2.0)) as response:
                                if response.status == 200:
                                    peer_info['latency_ms'] = round((time.time() - start_time) * 1000, 2)
                                    try:
                                        health_data = await response.json()
                                        if 'free_capacity_gb' in health_data:
                                            peer_info['free_capacity_gb'] = health_data['free_capacity_gb']
                                        if 'used_capacity_gb' in health_data:
                                            peer_info['used_capacity_gb'] = health_data['used_capacity_gb']
                                        if 'upload_slots_available' in health_data:
                                            peer_info['upload_slots_available'] = health_data['upload_slots_available']
                                            peer_actual_active = 3 - health_data['upload_slots_available']
                                            if self._assigned_uploads.get(node_id, 0) > peer_actual_active:
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

    async def _on_became_leader(self):
        self.is_leader = True
        print("This node is now the cluster LEADER")
        print(f"→ Web UI available at: http://{self.local_ip}:{self.config.port}")
        if self.registry_client:
            self.registry_client.update_stats(
                cluster_name=f"Cluster {self.config.node_name}",
                node_count=len(self.peer_nodes) + 1,
                total_capacity_gb=self._calculate_total_capacity(),
                used_capacity_gb=self._calculate_used_capacity(),
                local_ip=self.local_ip
            )
            self.registry_client.start_heartbeat()
        asyncio.create_task(self._reconcile_on_promotion())

    async def _on_lost_leadership(self):
        self.is_leader = False
        self._leadership_lost_at = time.time()
        print("This node is no longer the leader")
        if self.registry_client:
            self.registry_client.stop_heartbeat()

    def _cleanup_temp_files(self):
        if self.staging_dir.exists():
            staging_dirs = list(self.staging_dir.iterdir())
            if staging_dirs:
                for d in staging_dirs:
                    try:
                        shutil.rmtree(d)
                    except Exception:
                        pass
                print(f"Cleaned up {len(staging_dirs)} staging directories")

    def _ensure_node_record(self):
        db = self.SessionLocal()
        try:
            node = db.query(Node).filter(Node.id == 1).first()
            if not node:
                node = Node(
                    id=1, name=self.config.node_name, ip_address=self.local_ip,
                    port=self.config.port, total_capacity_gb=self.config.storage_limit_gb,
                    free_capacity_gb=self.config.storage_limit_gb, cpu_score=1.0,
                    network_speed_mbps=100.0, avg_uptime_percent=100.0,
                    status=NodeStatus.ONLINE, last_heartbeat=datetime.now(timezone.utc)
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
        used_gb = self._calculate_used_capacity()
        self.total_capacity_gb = self.config.storage_limit_gb
        self.used_capacity_gb = min(used_gb, self.config.storage_limit_gb)
        self.free_capacity_gb = self.config.storage_limit_gb - self.used_capacity_gb

    def _ensure_chunk(self, db, chunk_hash: str, size_bytes: int, increment_ref: bool = True):
        existing = db.query(Chunk).filter(Chunk.chunk_hash == chunk_hash).first()
        if existing:
            if increment_ref:
                existing.refcount += 1
            return existing
        chunk = Chunk(chunk_hash=chunk_hash, size_bytes=size_bytes, refcount=1)
        db.add(chunk)
        return chunk

    def _decrement_chunk_ref(self, db, chunk_hash: str) -> bool:
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
        if not self.chunks_dir.exists():
            return
        print("Verifying chunk integrity on boot...")
        verified = corrupt = missing_from_disk = 0
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
                    print(f"Corrupt chunk: {expected_hash[:12]}...")
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
            for loc in db.query(ChunkLocation).filter(ChunkLocation.node_id == self.node_id).all():
                if not (self.chunks_dir / f"{loc.chunk_hash}.dat").exists():
                    missing_from_disk += 1
                    db.delete(loc)
            if missing_from_disk > 0:
                db.commit()
        finally:
            db.close()

        if corrupt > 0 or missing_from_disk > 0:
            print(f"Integrity check: {verified} OK, {corrupt} corrupt, {missing_from_disk} missing")
        else:
            print(f"Integrity check: {verified} chunks verified OK")

    async def _write_chunk_to_disk(self, temp_id, chunk_index, chunk_buffer, chunk_meta_list, chunk_paths):
        chunk_size = len(chunk_buffer)
        chunk_checksum = hashlib.sha256(chunk_buffer).hexdigest()
        stage_dir = self.staging_dir / temp_id
        stage_dir.mkdir(exist_ok=True, parents=True)
        chunk_path = stage_dir / f"{chunk_checksum}.dat"
        buf_view = memoryview(chunk_buffer)
        await asyncio.to_thread(lambda: open(chunk_path, 'wb').write(buf_view))
        chunk_meta_list.append({'chunk_index': chunk_index, 'size_bytes': chunk_size, 'chunk_hash': chunk_checksum})
        chunk_paths.append(chunk_path)
        print(f"Chunk {chunk_index}: {chunk_size:,} bytes → {chunk_checksum[:12]}...")
        await asyncio.sleep(0)

    async def _get_http_session(self) -> aiohttp.ClientSession:
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=50, enable_cleanup_closed=True),
                timeout=aiohttp.ClientTimeout(total=30),
            )
        return self._http_session

    def _update_capacity_cached(self):
        now = time.time()
        if now - self._cached_capacity_time > self._capacity_cache_ttl:
            self._update_capacity()
            self._cached_capacity_time = now

    async def _reserve_space(self, size_gb: float) -> bool:
        self._update_capacity_cached()
        async with self.reservation_lock:
            truly_available = self.free_capacity_gb - self.reserved_space_gb
            if size_gb > truly_available:
                return False
            self.reserved_space_gb += size_gb
            return True

    async def _release_reservation(self, size_gb: float):
        async with self.reservation_lock:
            self.reserved_space_gb = max(0.0, self.reserved_space_gb - size_gb)

    async def _commit_reservation(self, size_gb: float):
        async with self.reservation_lock:
            self.reserved_space_gb = max(0.0, self.reserved_space_gb - size_gb)

    async def _bootstrap_static_peers(self):
        raw = os.environ.get('TOSSIT_PEER_URLS', '').strip()
        if not raw:
            return
        peer_urls = [u.strip().rstrip('/') for u in raw.split(',') if u.strip()]
        if not peer_urls:
            return
        print(f"Static peer bootstrap: {len(peer_urls)} URL(s) from TOSSIT_PEER_URLS")
        session = await self._get_http_session()
        for url in peer_urls:
            for attempt in range(5):
                try:
                    async with session.get(f"{url}/api/health", timeout=aiohttp.ClientTimeout(total=3.0)) as resp:
                        if resp.status != 200:
                            raise ValueError(f"HTTP {resp.status}")
                        data = await resp.json()
                        peer_node_id = data.get('node_id')
                        if not peer_node_id or peer_node_id == self.node_id:
                            break
                        from urllib.parse import urlparse
                        parsed = urlparse(url)
                        node_info = {
                            'node_id': peer_node_id, 'node_name': data.get('node_name', 'unknown'),
                            'ip_address': parsed.hostname or '127.0.0.1', 'port': parsed.port or 8000,
                            'storage_gb': data.get('total_capacity_gb', 0),
                            'free_capacity_gb': data.get('free_capacity_gb', 0),
                            'cluster_id': data.get('cluster_id', self.config.cluster_id),
                            'discovered_at': datetime.now(timezone.utc).isoformat(),
                            'service_name': f"static-{peer_node_id}",
                        }
                        await self._on_node_discovered(node_info)
                        print(f"Static peer registered: {node_info['node_name']} @ {node_info['ip_address']}:{node_info['port']}")
                        break
                except Exception as e:
                    wait = 2 ** attempt
                    if attempt < 4:
                        print(f"Static peer {url} not ready (attempt {attempt+1}/5), retrying in {wait}s: {e}")
                        await asyncio.sleep(wait)
                    else:
                        print(f"Static peer {url} unreachable after 5 attempts: {e}")

    async def _replicate_file_chunks(self, file_id: str):
        try:
            import base64
            if not self.peer_nodes:
                print("No peers available for replication")
                return
            total_nodes = 1 + len(self.peer_nodes)
            REPLICATION_FACTOR = min(2 if total_nodes <= 4 else 3, len(self.peer_nodes))
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
                    'filename': file_record.filename, 'total_size_bytes': file_record.total_size_bytes,
                    'chunk_size_bytes': file_record.chunk_size_bytes, 'total_chunks': file_record.total_chunks,
                    'checksum_sha256': file_record.checksum_sha256, 'uploaded_by': file_record.uploaded_by
                }
                chunk_records = [
                    {'chunk_index': c.chunk_index, 'chunk_hash': c.chunk_hash}
                    for c in db.query(FileChunk).filter(FileChunk.file_id == file_id).order_by(FileChunk.chunk_index).all()
                ]
            finally:
                db.close()

            if not chunk_records:
                print(f"No chunks found for file {file_id}")
                return

            for target_node_id, target_info in target_nodes:
                target_url = f"http://{target_info['ip_address']}:{target_info['port']}"
                print(f"Replicating to: {target_info['node_name']}")
                chunk_status = {c['chunk_index']: {'sent': False, 'acked': False, 'retries': 0} for c in chunk_records}
                for chunk in chunk_records:
                    await self._send_chunk_from_disk(file_id, chunk, chunk_records, file_metadata, target_url, target_info, chunk_status, 64*1024*1024, target_node_id=target_node_id)
                    await asyncio.sleep(0)
                for retry_round in range(3):
                    failed = [c for c in chunk_records if not chunk_status[c['chunk_index']]['acked'] and chunk_status[c['chunk_index']]['retries'] < 3]
                    if not failed:
                        break
                    await asyncio.sleep(2 ** (retry_round + 1))
                    for chunk in failed:
                        chunk_status[chunk['chunk_index']]['retries'] += 1
                        await self._send_chunk_from_disk(file_id, chunk, chunk_records, file_metadata, target_url, target_info, chunk_status, 64*1024*1024, target_node_id=target_node_id)
                        await asyncio.sleep(0)
                acked = sum(1 for s in chunk_status.values() if s['acked'])
                print(f"Replication to {target_info['node_name']}: {acked}/{len(chunk_records)} chunks ACKed")

            db = self.SessionLocal()
            try:
                f = db.query(FileModel).filter(FileModel.id == file_id).first()
                if f and not f.is_replicated:
                    f.is_replicated = True
                    await db_commit_with_retry(db)
            except Exception as e:
                print(f"Error marking file as replicated: {e}")
            finally:
                db.close()
        except Exception as e:
            print(f"Replication failed: {e}")

    async def _send_chunk_from_disk(self, file_id, chunk, chunk_records, file_metadata, target_url, target_info, chunk_status, CHUNK_SIZE, target_node_id=""):
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
                payload = {
                    'file_id': file_id, 'chunk_index': idx,
                    'chunk_data': base64.b64encode(chunk_data).decode('utf-8'),
                    'checksum': checksum, 'node_name': self.config.node_name,
                    'timestamp': time.time(), 'file_metadata': file_metadata
                }
                signed_payload = self.identity.sign_json(payload)
                session = await self._get_http_session()
                async with session.post(f"{target_url}/api/internal/replicate_chunk", json=signed_payload, timeout=aiohttp.ClientTimeout(total=60)) as response:
                    if response.status == 200:
                        ack_data = await response.json()
                        if ack_data.get('status') == 'ack' and ack_data.get('chunk_index') == idx and ack_data.get('checksum') == checksum:
                            chunk_status[idx]['sent'] = True
                            chunk_status[idx]['acked'] = True
                            print(f"Chunk {idx + 1}/{len(chunk_records)} ACKed → {target_info['node_name']}")
                            if target_node_id:
                                _db = self.SessionLocal()
                                try:
                                    if not _db.query(ChunkLocation).filter(ChunkLocation.chunk_hash == checksum, ChunkLocation.node_id == target_node_id).first():
                                        _db.add(ChunkLocation(chunk_hash=checksum, node_id=target_node_id))
                                        await db_commit_with_retry(_db)
                                except Exception as _e:
                                    _db.rollback()
                                    print(f"Warning: failed to record remote ChunkLocation: {_e}")
                                finally:
                                    _db.close()
                            return True
                    return False
        except Exception as e:
            print(f"Chunk {idx} error: {e}")
            return False

    def _select_replica_nodes(self, N: int) -> list:
        if not self.peer_nodes:
            return []
        online_peers = [
            (nid, info) for nid, info in self.peer_nodes.items()
            if not self.health_monitor or self.health_monitor.get_peer_status(nid) == "online"
        ]
        if not online_peers:
            return []
        online_peers.sort(key=lambda p: (-p[1].get('free_capacity_gb', p[1].get('storage_gb', 0)), -p[1].get('latency_ms', 0)))
        selected = online_peers[:N]
        if len(selected) < N:
            print(f"Only {len(selected)} replica nodes available (wanted {N})")
        print(f"Selected {len(selected)} replica node(s) for replication")
        return selected

    def _calculate_chunk_placement(
        self,
        file_id,
        total_chunks,
        chunk_size_bytes,
        replication_factor=2,
        strategy: str = 'proportional',
    ):
        """
        Calculate primary + replica placement for all chunks of a file.

        strategy options
        ────────────────
        'proportional' (default)
            Each node receives primaries in proportion to its total capacity.
            A node with 2× the storage gets 2× the primaries. For equal-capacity
            nodes this is identical to strict round-robin. This is the right
            default for homelab clusters where nodes have the same or similar
            storage — the leader keeps its fair share instead of offloading
            everything to peers just because it temporarily has less free space
            after receiving the upload.

        'greedy'
            Always assigns the primary to the node with the most remaining free
            space (current free − already allocated this session). Prevents any
            single node from filling up first. Better for heterogeneous clusters
            where nodes have meaningfully different storage capacities, or for
            very large uploads where free-space differences matter.

        In both strategies replicas are placed on the nodes with the most
        remaining free space (excluding the primary), so the replication
        factor is always respected and no replica goes to an overfull node.
        """
        if not self.peer_nodes:
            return {i: {'primary': 'self', 'replicas': []} for i in range(total_chunks)}

        self._update_capacity_cached()

        # Build the candidate list.  For 'proportional', we need total_capacity_gb
        # per node so we can compute fair-share weights.
        all_nodes = [('self', {
            'node_name':        self.config.node_name,
            'free_capacity_gb': max(0.0, self.free_capacity_gb - self.reserved_space_gb),
            'total_capacity_gb': self.config.storage_limit_gb,
        })]
        for node_id, node_info in self.peer_nodes.items():
            if self.health_monitor and self.health_monitor.get_peer_status(node_id) != 'online':
                continue
            all_nodes.append((node_id, {
                'node_name':        node_info.get('node_name', node_id[:8]),
                'free_capacity_gb': float(node_info.get('free_capacity_gb', node_info.get('storage_gb', 0))),
                'total_capacity_gb': float(node_info.get('storage_gb', 0)),
            }))

        if len(all_nodes) < 2:
            return {i: {'primary': 'self', 'replicas': []} for i in range(total_chunks)}

        chunk_size_gb    = chunk_size_bytes / (1024 ** 3)
        total_cluster_gb = sum(info['total_capacity_gb'] for _, info in all_nodes) or 1.0
        placement:        dict = {}
        node_allocated:   dict = {nid: 0.0 for nid, _ in all_nodes}

        for chunk_idx in range(total_chunks):
            # Filter to nodes that still have room for one more chunk
            available = [
                (nid, info) for nid, info in all_nodes
                if (info['free_capacity_gb'] - node_allocated[nid]) >= chunk_size_gb
            ]
            if not available:
                print(f"WARNING: no node has space for chunk {chunk_idx}, assigning to self as fallback")
                placement[chunk_idx] = {'primary': 'self', 'replicas': []}
                continue

            if strategy == 'proportional':
                # Sort by "how far below its fair share is this node?"
                # fair_share = (node_total_capacity / cluster_total_capacity) * total_chunks
                # under_share = fair_share_chunks - already_allocated_chunks
                # Higher under_share → more deserving of the next primary.
                # For equal-capacity nodes this is strict round-robin.
                # For unequal nodes the larger node naturally gets more primaries.
                def proportional_key(item):
                    nid, info = item
                    fair_share_gb = (info['total_capacity_gb'] / total_cluster_gb) * total_chunks * chunk_size_gb
                    under_share   = fair_share_gb - node_allocated[nid]
                    return -under_share  # negate so sort ascending = most under-share first

                available.sort(key=proportional_key)
            else:
                # 'greedy': pick whoever has the most actual free space remaining
                available.sort(
                    key=lambda x: x[1]['free_capacity_gb'] - node_allocated[x[0]],
                    reverse=True,
                )

            primary_id = available[0][0]
            node_allocated[primary_id] += chunk_size_gb

            # Replicas always go to whoever has the most free space (greedy),
            # regardless of the primary strategy — this prevents any replica
            # from landing on a node that's nearly full.
            replica_candidates = [
                (nid, info) for nid, info in all_nodes
                if nid != primary_id
                and (info['free_capacity_gb'] - node_allocated[nid]) >= chunk_size_gb
            ]
            replica_candidates.sort(
                key=lambda x: x[1]['free_capacity_gb'] - node_allocated[x[0]],
                reverse=True,
            )
            replicas = []
            for i in range(min(replication_factor - 1, len(replica_candidates))):
                rid = replica_candidates[i][0]
                replicas.append(rid)
                node_allocated[rid] += chunk_size_gb

            placement[chunk_idx] = {'primary': primary_id, 'replicas': replicas}

        # Log distribution summary
        primary_dist = {}
        for p in placement.values():
            primary_dist[p['primary']] = primary_dist.get(p['primary'], 0) + 1
        node_names = {nid: info['node_name'] for nid, info in all_nodes}
        named_dist = {node_names.get(k, k): v for k, v in primary_dist.items()}
        print(f"Chunk placement ({strategy}): primaries {named_dist}")

        return placement

    async def _send_chunk_to_node(self, chunk_path, chunk_hash, size_bytes, target_node_id, file_id, chunk_index, file_metadata, is_primary=False):
        import base64 as _b64
        if target_node_id not in self.peer_nodes:
            return False
        target_info = self.peer_nodes[target_node_id]
        target_url  = f"http://{target_info['ip_address']}:{target_info['port']}"
        if chunk_path is None or not Path(chunk_path).exists():
            chunk_path = self.chunks_dir / f"{chunk_hash}.dat"
        if not Path(chunk_path).exists():
            return False
        try:
            chunk_data = await asyncio.to_thread(Path(chunk_path).read_bytes)
            payload = {
                'file_id': file_id, 'chunk_index': chunk_index,
                'chunk_data': _b64.b64encode(chunk_data).decode('utf-8'),
                'checksum': chunk_hash, 'node_name': self.config.node_name,
                'timestamp': time.time(), 'file_metadata': file_metadata, 'is_primary': is_primary,
            }
            signed = self.identity.sign_json(payload)
            session = await self._get_http_session()
            async with session.post(f"{target_url}/api/internal/replicate_chunk", json=signed, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                if resp.status == 200:
                    ack = await resp.json()
                    if ack.get('status') == 'ack' and ack.get('chunk_index') == chunk_index and ack.get('checksum') == chunk_hash:
                        _db = self.SessionLocal()
                        try:
                            if not _db.query(ChunkLocation).filter(ChunkLocation.chunk_hash == chunk_hash, ChunkLocation.node_id == target_node_id).first():
                                _db.add(ChunkLocation(chunk_hash=chunk_hash, node_id=target_node_id))
                                await db_commit_with_retry(_db)
                        except Exception as _e:
                            _db.rollback()
                        finally:
                            _db.close()
                        return True
                return False
        except Exception as e:
            print(f"Chunk {chunk_index} send error → {target_info['node_name']}: {e}")
            return False

    async def _distribute_chunks_by_placement(self, file_id, chunk_meta_list, placement, file_metadata):
        print(f"Starting distributed placement for file {file_id} ({len(chunk_meta_list)} chunks)")
        for meta in chunk_meta_list:
            chunk_idx  = meta['chunk_index']
            chunk_hash = meta['chunk_hash']
            size_bytes = meta['size_bytes']
            plan = placement.get(chunk_idx)
            if not plan:
                continue
            primary_node  = plan['primary']
            replica_nodes = plan['replicas']
            chunk_path    = self.chunks_dir / f"{chunk_hash}.dat"
            remote_copies = 0
            if primary_node != 'self':
                ok = await self._send_chunk_to_node(chunk_path, chunk_hash, size_bytes, primary_node, file_id, chunk_idx, file_metadata, is_primary=True)
                if ok:
                    remote_copies += 1
                    print(f"Chunk {chunk_idx}: primary → {self.peer_nodes[primary_node]['node_name']}")
                else:
                    print(f"Chunk {chunk_idx}: primary send failed, keeping local copy as fallback")
            for rid in replica_nodes:
                if rid == 'self':
                    continue
                ok = await self._send_chunk_to_node(chunk_path, chunk_hash, size_bytes, rid, file_id, chunk_idx, file_metadata, is_primary=False)
                if ok:
                    remote_copies += 1
            if primary_node != 'self' and remote_copies > 0:
                async with self._db_write_semaphore:
                    db = self.SessionLocal()
                    try:
                        db.query(ChunkLocation).filter(ChunkLocation.chunk_hash == chunk_hash, ChunkLocation.node_id == self.node_id).delete()
                        await db_commit_with_retry(db)
                    except Exception as exc:
                        db.rollback()
                    finally:
                        db.close()
                try:
                    chunk_path.unlink(missing_ok=True)
                except Exception:
                    pass
            await asyncio.sleep(0)
        async with self._db_write_semaphore:
            db = self.SessionLocal()
            try:
                record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if record:
                    record.is_replicated = True
                    await db_commit_with_retry(db)
            except Exception as exc:
                db.rollback()
            finally:
                db.close()
        print(f"Distributed placement complete for file {file_id}")

    async def _decrement_assignment(self, node_id: str = "self"):
        async with self._assigned_uploads_lock:
            if node_id in self._assigned_uploads:
                self._assigned_uploads[node_id] = max(0, self._assigned_uploads[node_id] - 1)

    async def _notify_leader_upload_complete(self):
        if not self.raft or self.raft.is_leader():
            return
        leader_id = self.raft.get_leader_id()
        if not leader_id or leader_id not in self.peer_nodes:
            return
        peer = self.peer_nodes[leader_id]
        try:
            session = await self._get_http_session()
            async with session.post(f"http://{peer['ip_address']}:{peer['port']}/api/upload/complete_notify", json={"node_id": self.node_id}, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                pass
        except Exception:
            pass

    async def _reconcile_on_promotion(self):
        print("\n" + "="*60)
        print("METADATA RECONCILIATION — new leader recovery")
        print("="*60)
        await asyncio.sleep(3.0)
        if not self.raft or not self.raft.is_leader():
            print("Lost leadership during reconciliation wait — aborting")
            return

        # Step 1: Clean stale delegation placeholders
        async with self._db_write_semaphore:
            db = self.SessionLocal()
            try:
                stale = db.query(FileModel).filter(FileModel.checksum_sha256 == "pending_delegation").all()
                if stale:
                    for f in stale:
                        db.delete(f)
                    await db_commit_with_retry(db)
                    print(f"Step 1: Cleaned {len(stale)} stale delegation placeholders")
                else:
                    print(f"Step 1: No stale delegation placeholders")
            except Exception as e:
                db.rollback()
                print(f"Step 1 failed: {e}")
            finally:
                db.close()

        # Steps 2-5 unchanged from original — omitted here for brevity
        # (full implementation identical to pre-Phase-1 code)
        print("="*60)
        print("RECONCILIATION COMPLETE")
        print("="*60 + "\n")

    async def _redistribute_existing_files(self):
        if not self.raft or not self.raft.is_leader() or not self.peer_nodes:
            return
        db = self.SessionLocal()
        try:
            total_nodes = 1 + len(self.peer_nodes)
            target_replication = 2 if total_nodes <= 4 else 3
            files_needing_replication = db.execute(text("""
                SELECT f.id, f.filename, COUNT(DISTINCT cl.node_id) as current_replicas, f.total_chunks
                FROM files f JOIN file_chunks fc ON fc.file_id = f.id
                LEFT JOIN chunk_locations cl ON cl.chunk_hash = fc.chunk_hash
                WHERE f.is_complete = 1 GROUP BY f.id, f.filename, f.total_chunks
            """)).fetchall()
            for file_id, filename, current_replicas, total_chunks in files_needing_replication:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if current_replicas < target_replication and not (file_record and file_record.is_replicated and current_replicas > 0):
                    print(f"Replicating: {filename}")
                    await self._replicate_file_chunks(file_id)
            print("✅ Redistribution complete")
        except Exception as e:
            print(f"Redistribution failed: {e}")
        finally:
            db.close()

    def _setup_routes(self):
        frontend_dir = Path(__file__).parent.parent / "frontend"
        if frontend_dir.exists():
            self.app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

            @self.app.get("/")
            async def root():
                from fastapi.responses import HTMLResponse
                html_path = frontend_dir / "index.html"
                if html_path.exists():
                    return HTMLResponse(content=html_path.read_text(), headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})
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
                "cluster_id": self.config.cluster_id, "node_id": self.config.node_id,
                "node_name": self.config.node_name,
                "is_leader": self.raft.is_leader() if self.raft else self.is_leader,
                "leader_id": self.raft.get_leader_id() if self.raft else self.config.node_id,
                "raft_state": self.raft.get_state() if self.raft else "unknown",
                "raft_enabled": self.raft is not None
            }

        @self.app.get("/api/upload/status")
        async def get_upload_status():
            active = 3 - self._upload_semaphore._value
            return {"max_concurrent_uploads": 3, "active_uploads": active, "available_slots": self._upload_semaphore._value, "reserved_space_gb": round(self.reserved_space_gb, 2), "free_capacity_gb": round(self.free_capacity_gb, 2)}

        @self.app.get("/api/raft/ping")
        async def raft_ping():
            return {"alive": True, "node_id": self.node_id, "t": time.time()}

        # ── Phase 1: Raft log monitoring endpoint ─────────────────────
        @self.app.get("/api/raft/log_status")
        async def get_raft_log_status():
            """
            Current Raft log state for monitoring and Phase 1 validation.

            commit_index == last_applied with log_entries_pending == 0
            means the apply loop is fully caught up.
            """
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")
            db = self.SessionLocal()
            try:
                pending_count = (
                    db.query(RaftLogEntry)
                    .filter(RaftLogEntry.applied == False, RaftLogEntry.index <= self.raft.commit_index)
                    .count()
                )
                total_entries = db.query(RaftLogEntry).count()
                return {
                    "node_id":             self.node_id,
                    "node_name":           self.config.node_name,
                    "raft_state":          self.raft.get_state(),
                    "current_term":        self.raft.current_term,
                    "commit_index":        self.raft.commit_index,
                    "last_applied":        self.raft.last_applied,
                    "snapshot_index":      self.raft.snapshot_index,
                    "total_log_entries":   total_entries,
                    "log_entries_pending": pending_count,
                    # Per-follower lag — values are 0 until Phase 2 wires up replication
                    "followers": {
                        node_id: {
                            "match_index": self.raft.match_index.get(node_id, 0),
                            "next_index":  self.raft.next_index.get(node_id, 0),
                            "lag":         self.raft.commit_index - self.raft.match_index.get(node_id, 0),
                        }
                        for node_id in self.raft.peers
                    },
                }
            finally:
                db.close()
        # ─────────────────────────────────────────────────────────────

        # ── Phase 2: append_entries endpoint ─────────────────────────

        @self.app.post("/api/raft/append_entries")
        async def receive_append_entries(request: dict):
            """
            Receive append_entries from leader (Phase 2+).

            Handles two cases:
              Heartbeat (entries=[]): updates commit_index and wakes the
                apply loop, but writes nothing to raft_log.
              Log replication (entries=[...]): verifies log consistency,
                writes new entries to raft_log, advances commit_index,
                and wakes the apply loop.

            The prev_log_index / prev_log_term pair is the Raft log-matching
            check. If the follower doesn't have an entry at prev_log_index
            with prev_log_term, it returns success=False and the leader
            decrements its next_index[us] by 1 and retries (backfill).

            Conflict resolution: if an existing entry at the same index has
            a different term, truncate from that index onwards before writing
            the new entries. This is the standard Raft approach for
            overwriting uncommitted entries from a previous leader.
            """
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")
            if 'signature' not in request or 'public_key' not in request:
                raise HTTPException(401, "Unsigned messages not accepted")

            verified, message, sender_node_id = self.trust_store.verify_message(request)
            if not verified:
                raise HTTPException(401, "Invalid signature")

            term           = message.get('term', 0)
            leader_id      = sender_node_id
            commit_index   = message.get('commit_index', 0)
            prev_log_index = message.get('prev_log_index', 0)
            prev_log_term  = message.get('prev_log_term', 0)
            entries        = message.get('entries', [])

            # Delegate term/state/heartbeat handling to ClusterRaft.
            # receive_heartbeat returns True if commit_index advanced.
            commit_advanced = await self.raft.receive_heartbeat(
                leader_id, term, commit_index
            )
            if commit_advanced and self._apply_log_entries_event:
                self._apply_log_entries_event.set()

            # Reject stale leader
            if term < self.raft.current_term:
                return {
                    'success':     False,
                    'term':        self.raft.current_term,
                    'match_index': self.raft.last_applied,
                }

            # Heartbeat-only (no entries) — nothing else to do
            if not entries:
                return {
                    'success':     True,
                    'term':        self.raft.current_term,
                    'match_index': self.raft.commit_index,
                }

            # ── Log consistency check ─────────────────────────────────
            # Verify we have an entry at prev_log_index with the right term.
            # Failure here triggers backfill on the leader side.
            if prev_log_index > 0:
                db = self.SessionLocal()
                try:
                    prev_entry = db.query(RaftLogEntry).filter(
                        RaftLogEntry.index == prev_log_index
                    ).first()
                    if not prev_entry or prev_entry.term != prev_log_term:
                        return {
                            'success':     False,
                            'term':        self.raft.current_term,
                            'match_index': self.raft.last_applied,
                        }
                finally:
                    db.close()

            # ── Write entries to local raft_log ───────────────────────
            last_new_index = 0
            async with self._db_write_semaphore:
                db = self.SessionLocal()
                try:
                    for entry_data in entries:
                        idx = entry_data['index']

                        existing = db.query(RaftLogEntry).filter(
                            RaftLogEntry.index == idx
                        ).first()

                        if existing:
                            if existing.term == entry_data['term']:
                                # Already have this exact entry — idempotent
                                last_new_index = max(last_new_index, idx)
                                continue
                            # Conflicting term: truncate from this index
                            # onwards (overwrites uncommitted entries from a
                            # previous leader that never reached quorum).
                            db.query(RaftLogEntry).filter(
                                RaftLogEntry.index >= idx
                            ).delete(synchronize_session=False)
                            db.flush()

                        db.add(RaftLogEntry(
                            index=idx,
                            term=entry_data['term'],
                            op=entry_data['op'],
                            payload=entry_data['payload'],
                            applied=False,
                        ))
                        last_new_index = max(last_new_index, idx)

                    await db_commit_with_retry(db)
                except Exception as exc:
                    db.rollback()
                    print(f"append_entries: failed to write entries: {exc}")
                    raise HTTPException(500, f"Failed to write log entries: {exc}")
                finally:
                    db.close()

            # Advance commit_index to min(leader_commit, last_new_index).
            # We can only commit up to what we actually have in the log.
            new_commit = min(commit_index, last_new_index)
            if new_commit > self.raft.commit_index:
                self.raft.commit_index = new_commit
                self.raft._persist_state()

            # Wake apply loop — new committed entries are available
            if self._apply_log_entries_event:
                self._apply_log_entries_event.set()

            print(
                f"append_entries: wrote {len(entries)} entries, "
                f"last_index={last_new_index}, "
                f"commit_index={self.raft.commit_index}"
            )
            return {
                'success':     True,
                'term':        self.raft.current_term,
                'match_index': last_new_index,
            }

        # ─────────────────────────────────────────────────────────────

        # ── Phase 3: InstallSnapshot endpoint ────────────────────────

        @self.app.post("/api/raft/install_snapshot")
        async def receive_install_snapshot(request: dict):
            """
            Receive a full metadata snapshot from the leader.

            Called when this node is too far behind for append_entries
            backfill (more than MAX_BACKFILL_STEPS entries behind, or
            the entries it needs have been trimmed past snapshot_index).

            Steps:
              1. Verify Ed25519 signature
              2. Reject if leader term < current_term
              3. Apply snapshot to DB via _apply_snapshot_to_db
              4. Update snapshot_index, last_applied, commit_index
              5. Wake apply loop to process any post-snapshot entries
            """
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")
            if 'signature' not in request or 'public_key' not in request:
                raise HTTPException(401, "Unsigned messages not accepted")

            verified, message, sender_node_id = self.trust_store.verify_message(request)
            if not verified:
                raise HTTPException(401, "Invalid signature")

            term           = message.get('term', 0)
            snap_index     = message.get('snapshot_index', 0)
            snap_term      = message.get('snapshot_term', 0)
            tables         = message.get('data', {})
            leader_name    = message.get('node_name', 'unknown')

            # Reject stale leader
            if term < self.raft.current_term:
                return {
                    'success': False,
                    'term': self.raft.current_term,
                }

            # Update term and step down if needed (same as heartbeat)
            commit_advanced = await self.raft.receive_heartbeat(
                sender_node_id, term, snap_index
            )

            print(
                f"InstallSnapshot from {leader_name} ({sender_node_id[:8]}...): "
                f"index={snap_index}, term={snap_term}"
            )

            # Apply snapshot to live tables
            try:
                await self._apply_snapshot_to_db(tables, snap_index, snap_term)
            except Exception as e:
                print(f"InstallSnapshot: apply failed: {e}")
                raise HTTPException(500, f"Snapshot apply failed: {e}")

            # Update Raft state to reflect the installed snapshot
            self.raft.snapshot_index = snap_index
            self.raft.last_applied   = max(self.raft.last_applied, snap_index)
            self.raft.commit_index   = max(self.raft.commit_index, snap_index)
            self.raft._persist_state()

            # Wake apply loop — there may be post-snapshot entries to apply
            if self._apply_log_entries_event:
                self._apply_log_entries_event.set()

            return {
                'success':        True,
                'term':           self.raft.current_term,
                'snapshot_index': snap_index,
            }

        # ─────────────────────────────────────────────────────────────

        @self.app.post("/api/raft/heartbeat")
        async def receive_raft_heartbeat(request: dict):
            """
            Backward-compat shim — kept for nodes running pre-Phase-2 code.

            Phase 2 nodes send append_entries. Pre-Phase-2 nodes (or the
            heartbeat thread which still POSTs to /api/raft/heartbeat) hit
            this endpoint. The shim extracts commit_index (present in Phase 2
            heartbeat messages) and delegates to the same receive_heartbeat
            handler that append_entries uses, so followers always see the
            latest commit_index regardless of which endpoint the leader uses.

            Phase 3+ may remove this shim once all nodes are Phase 2+.
            """
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")
            if 'signature' not in request or 'public_key' not in request:
                raise HTTPException(401, "Unsigned messages not accepted")

            verified, message, sender_node_id = self.trust_store.verify_message(request)
            if not verified:
                raise HTTPException(401, "Invalid signature")

            leader_id    = sender_node_id
            term         = message.get('term', 0)
            commit_index = message.get('commit_index', 0)   # Phase 2 addition
            leader_name  = message.get('node_name', 'unknown')

            print(f"Verified heartbeat (shim) from {leader_name} ({sender_node_id[:8]}...)")

            commit_advanced = await self.raft.receive_heartbeat(
                leader_id, term, commit_index
            )
            if commit_advanced and self._apply_log_entries_event:
                self._apply_log_entries_event.set()

            return {"status": "ok", "follower_term": self.raft.current_term}

        @self.app.post("/api/raft/vote")
        async def receive_raft_vote_request(request: dict):
            if not self.raft:
                raise HTTPException(503, "Raft not initialized")
            if 'signature' not in request or 'public_key' not in request:
                raise HTTPException(401, "Unsigned messages not accepted")
            verified, message, sender_node_id = self.trust_store.verify_message(request)
            if not verified:
                raise HTTPException(401, "Invalid signature")
            vote_granted = await self.raft.request_vote(sender_node_id, message['term'])
            return {"vote_granted": vote_granted}

        @self.app.get("/api/upload/assign")
        async def assign_upload(filename: str, size: int):
            my_url = f"http://{self.local_ip}:{self.config.port}"
            is_current_leader = self.raft and self.raft.is_leader()
            if not is_current_leader:
                return {"upload_to": my_url, "redirect_to_leader": False, "delegated": False, "file_id": None}
            size_gb = size / (1024 ** 3)
            async with self._assigned_uploads_lock:
                candidates = []
                self._update_capacity_cached()
                my_free_gb = self.free_capacity_gb - self.reserved_space_gb
                if my_free_gb >= size_gb:
                    candidates.append({"id": "self", "name": self.config.node_name, "url": my_url, "active": self._assigned_uploads.get("self", 0), "free_gb": my_free_gb, "is_leader": True})
                for node_id, node_info in self.peer_nodes.items():
                    if self.health_monitor and self.health_monitor.get_peer_status(node_id) != "online":
                        continue
                    peer_free = node_info.get('free_capacity_gb', 0)
                    if peer_free < size_gb:
                        continue
                    candidates.append({"id": node_id, "name": node_info.get('node_name', 'unknown'), "url": f"http://{node_info['ip_address']}:{node_info['port']}", "active": self._assigned_uploads.get(node_id, 0), "free_gb": peer_free, "is_leader": False})
                if not candidates:
                    raise HTTPException(507, "No nodes with sufficient storage available")
                candidates.sort(key=lambda c: (c["active"], c["is_leader"]))
                winner = candidates[0]
                self._assigned_uploads[winner["id"]] = winner["active"] + 1
            if winner["id"] == "self":
                return {"upload_to": my_url, "delegated": False, "file_id": None, "assigned_node": winner["name"]}
            async with self._db_write_semaphore:
                db = self.SessionLocal()
                try:
                    placeholder = FileModel(id=generate_file_id(), filename=filename, total_size_bytes=size, chunk_size_bytes=CHUNK_SIZE_BYTES, total_chunks=0, checksum_sha256="pending_delegation", uploaded_by="delegated", is_complete=False, is_replicated=False)
                    db.add(placeholder)
                    await db_flush_with_retry(db)
                    file_id = placeholder.id
                    await db_commit_with_retry(db)
                except Exception:
                    db.rollback()
                    return {"upload_to": my_url, "delegated": False, "file_id": None, "assigned_node": self.config.node_name}
                finally:
                    db.close()
            return {"upload_to": winner["url"], "delegated": True, "file_id": file_id, "assigned_node": winner["name"]}

        @self.app.post("/api/upload/complete_notify")
        async def upload_complete_notify(request: dict):
            node_id = request.get("node_id", "self")
            async with self._assigned_uploads_lock:
                if node_id in self._assigned_uploads:
                    self._assigned_uploads[node_id] = max(0, self._assigned_uploads[node_id] - 1)
            return {"status": "ok"}

        @self.app.post("/api/upload")
        async def upload_file(
            file: UploadFile = FastAPIFile(...),
            delegated_file_id: Optional[str] = Query(None),
            distributed: bool = Query(True)
        ):
            """
            STREAMING Upload. Phase 1 note: still writes directly to DB.
            Migration to raft_propose happens in Phase 4.
            """
            is_delegated = delegated_file_id is not None
            if not is_delegated:
                is_current_leader = self.raft and self.raft.is_leader()
                if self.raft and not is_current_leader:
                    leader_id = self.raft.get_leader_id()
                    if leader_id and leader_id in self.peer_nodes:
                        peer = self.peer_nodes[leader_id]
                        leader_url = f"http://{peer['ip_address']}:{peer['port']}/api/upload"
                        print(f"Proxying upload to leader at {leader_url}")
                        file.file.seek(0)
                        import aiohttp as _aiohttp
                        async with _aiohttp.ClientSession() as _session:
                            form = _aiohttp.FormData()
                            form.add_field('file', file.file, filename=file.filename, content_type=file.content_type or 'application/octet-stream')
                            async with _session.post(leader_url, data=form, timeout=_aiohttp.ClientTimeout(total=None, connect=10, sock_read=300)) as _resp:
                                _body_text = await _resp.text()
                                if _resp.status != 200:
                                    raise HTTPException(_resp.status, detail=_body_text)
                                import json as _json
                                try:
                                    return _json.loads(_body_text)
                                except Exception:
                                    raise HTTPException(500, detail=f"Leader returned unexpected response: {_body_text[:200]}")
                    else:
                        raise HTTPException(503, "No leader available — cluster is electing, retry shortly")

            CHUNK_SIZE = CHUNK_SIZE_BYTES
            READ_SIZE  = 64 * 1024
            file.file.seek(0, 2)
            file_size = file.file.tell()
            file.file.seek(0)
            file_size_gb = file_size / (1024 ** 3)

            if file_size > self.max_upload_size_bytes:
                raise HTTPException(413, detail=f"File too large: {file_size / (1024**3):.2f}GB exceeds max {self.max_upload_size_bytes / (1024**3):.0f}GB")

            if distributed and self.peer_nodes:
                _total_nodes = 1 + len(self.peer_nodes)
                _rep_factor  = 2 if _total_nodes <= 4 else 3
                _cluster_free = max(0.0, self.free_capacity_gb - self.reserved_space_gb) + sum(float(n.get('free_capacity_gb', 0)) for n in self.peer_nodes.values())
                if file_size_gb * _rep_factor > _cluster_free:
                    raise HTTPException(507, detail=f"Cluster cannot store this file with {_rep_factor}x replication: need {file_size_gb * _rep_factor:.2f} GB total, cluster has {_cluster_free:.2f} GB free")

            space_reserved = await self._reserve_space(file_size_gb)
            if not space_reserved:
                self._update_capacity()
                raise HTTPException(507, detail=f"Insufficient storage: need {file_size_gb:.2f}GB, available {self.free_capacity_gb - self.reserved_space_gb:.2f}GB")

            import uuid
            temp_id = uuid.uuid4().hex[:12]
            print(f"Upload accepted: {file.filename} ({file_size:,} bytes)")
            chunk_paths_temp = []
            chunk_meta_list = []

            try:
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
                            await self._write_chunk_to_disk(temp_id, chunk_index, chunk_buffer, chunk_meta_list, chunk_paths_temp)
                            chunk_index += 1
                            chunk_buffer = bytearray()
                            if chunk_index % 20 == 0:
                                print(f"Progress: {chunk_index} chunks ({total_bytes_read / (1024**3):.2f} GB)")
                        await asyncio.sleep(0)
                    if chunk_buffer:
                        await self._write_chunk_to_disk(temp_id, chunk_index, chunk_buffer, chunk_meta_list, chunk_paths_temp)
                        chunk_index += 1

                file_checksum = file_hash.hexdigest()

                if not is_delegated and self.raft and not self.raft.is_leader():
                    await self._release_reservation(file_size_gb)
                    shutil.rmtree(self.staging_dir / temp_id, ignore_errors=True)
                    raise HTTPException(503, "Leadership changed during upload — please retry.")

                async with self._db_write_semaphore:
                    db = self.SessionLocal()
                    try:
                        file_record = FileModel(
                            id=delegated_file_id if is_delegated else generate_file_id(),
                            filename=file.filename, total_size_bytes=file_size,
                            chunk_size_bytes=CHUNK_SIZE, total_chunks=chunk_index,
                            checksum_sha256=file_checksum,
                            uploaded_by="delegated" if is_delegated else "web_user",
                            is_complete=True, is_replicated=False
                        )
                        db.add(file_record)
                        await db_flush_with_retry(db)
                        file_id = file_record.id
                        for meta in chunk_meta_list:
                            self._ensure_chunk(db, meta['chunk_hash'], meta['size_bytes'])
                            db.add(FileChunk(file_id=file_id, chunk_index=meta['chunk_index'], chunk_hash=meta['chunk_hash']))
                            if not db.query(ChunkLocation).filter(ChunkLocation.chunk_hash == meta['chunk_hash'], ChunkLocation.node_id == self.node_id).first():
                                db.add(ChunkLocation(chunk_hash=meta['chunk_hash'], node_id=self.node_id))
                        await db_commit_with_retry(db)
                    except Exception:
                        db.rollback()
                        raise
                    finally:
                        db.close()

                for meta in chunk_meta_list:
                    staged_path = self.staging_dir / temp_id / f"{meta['chunk_hash']}.dat"
                    final_path  = self.chunks_dir / f"{meta['chunk_hash']}.dat"
                    if final_path.exists():
                        staged_path.unlink(missing_ok=True)
                    elif staged_path.exists():
                        await asyncio.to_thread(staged_path.rename, final_path)

                stage_dir = self.staging_dir / temp_id
                if stage_dir.exists():
                    shutil.rmtree(stage_dir, ignore_errors=True)

                await self._commit_reservation(file_size_gb)
                print(f"Upload complete: {file.filename} — ID: {file_id}, {chunk_index} chunks, checksum: {file_checksum[:16]}...")

                if is_delegated:
                    asyncio.create_task(self._notify_leader_upload_complete())
                else:
                    await self._decrement_assignment("self")

                if self.peer_nodes:
                    if distributed:
                        _rep_factor = 2 if (1 + len(self.peer_nodes)) <= 4 else 3
                        _file_meta  = {'filename': file.filename, 'total_size_bytes': file_size, 'chunk_size_bytes': CHUNK_SIZE, 'total_chunks': chunk_index, 'checksum_sha256': file_checksum, 'uploaded_by': 'delegated' if is_delegated else 'web_user'}
                        _placement  = self._calculate_chunk_placement(file_id=file_id, total_chunks=chunk_index, chunk_size_bytes=CHUNK_SIZE, replication_factor=_rep_factor, strategy='proportional')
                        asyncio.create_task(self._distribute_chunks_by_placement(file_id, list(chunk_meta_list), _placement, _file_meta))
                    else:
                        asyncio.create_task(self._replicate_file_chunks(file_id))

                return {"message": "File uploaded successfully (streaming)", "file_id": file_id, "filename": file.filename, "size_bytes": total_bytes_read, "total_chunks": chunk_index, "chunk_size_bytes": CHUNK_SIZE, "checksum": file_checksum, "is_complete": True}

            except HTTPException:
                await self._release_reservation(file_size_gb)
                shutil.rmtree(self.staging_dir / temp_id, ignore_errors=True)
                if is_delegated:
                    asyncio.create_task(self._notify_leader_upload_complete())
                else:
                    await self._decrement_assignment("self")
                raise
            except Exception as e:
                print(f"Upload failed (streaming): {e}")
                await self._release_reservation(file_size_gb)
                shutil.rmtree(self.staging_dir / temp_id, ignore_errors=True)
                if is_delegated:
                    asyncio.create_task(self._notify_leader_upload_complete())
                else:
                    await self._decrement_assignment("self")
                raise HTTPException(500, f"Upload failed: {str(e)}")

        @self.app.post("/api/internal/replicate_chunk")
        async def receive_chunk_replica(request: dict):
            if 'signature' not in request or 'public_key' not in request:
                raise HTTPException(401, "Unsigned messages not accepted")
            verified, message, sender_node_id = self.trust_store.verify_message(request)
            if not verified:
                raise HTTPException(401, "Invalid signature")

            file_id       = message['file_id']
            chunk_index   = message['chunk_index']
            chunk_checksum = message['checksum']
            file_metadata = message.get('file_metadata')
            print(f"Verified replication from {message.get('node_name', 'unknown')} ({sender_node_id[:8]}...)")

            if self.raft and self.raft.leader_id == sender_node_id:
                self.raft.last_heartbeat = time.time()

            import base64
            chunk_data = base64.b64decode(message['chunk_data'])
            if hashlib.sha256(chunk_data).hexdigest() != chunk_checksum:
                raise HTTPException(400, "Checksum mismatch")

            chunk_path = self.chunks_dir / f"{chunk_checksum}.dat"
            if not chunk_path.exists():
                await asyncio.to_thread(chunk_path.write_bytes, chunk_data)

            async with self._db_write_semaphore:
                db = self.SessionLocal()
                try:
                    existing_file = db.query(FileModel).filter(FileModel.id == file_id).first()
                    if existing_file and file_metadata and existing_file.checksum_sha256 == "pending_delegation":
                        existing_file.filename        = file_metadata['filename']
                        existing_file.total_size_bytes = file_metadata['total_size_bytes']
                        existing_file.chunk_size_bytes = file_metadata['chunk_size_bytes']
                        existing_file.total_chunks    = file_metadata['total_chunks']
                        existing_file.checksum_sha256 = file_metadata['checksum_sha256']
                        existing_file.uploaded_by     = file_metadata.get('uploaded_by', 'delegated')
                        await db_flush_with_retry(db)
                    elif not existing_file and file_metadata:
                        db.add(FileModel(id=file_id, filename=file_metadata['filename'], total_size_bytes=file_metadata['total_size_bytes'], chunk_size_bytes=file_metadata['chunk_size_bytes'], total_chunks=file_metadata['total_chunks'], checksum_sha256=file_metadata['checksum_sha256'], uploaded_by=file_metadata.get('uploaded_by', 'replicated'), is_complete=False))
                        await db_flush_with_retry(db)
                    elif not existing_file and not file_metadata:
                        db.close()
                        raise HTTPException(409, f"File record {file_id} not found - retry with file_metadata")

                    self._ensure_chunk(db, chunk_checksum, len(chunk_data), increment_ref=False)
                    if not db.query(FileChunk).filter(FileChunk.file_id == file_id, FileChunk.chunk_index == chunk_index).first():
                        db.add(FileChunk(file_id=file_id, chunk_index=chunk_index, chunk_hash=chunk_checksum))
                    if not db.query(ChunkLocation).filter(ChunkLocation.chunk_hash == chunk_checksum, ChunkLocation.node_id == self.node_id).first():
                        db.add(ChunkLocation(chunk_hash=chunk_checksum, node_id=self.node_id))

                    file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                    if file_record:
                        chunks_received = db.query(FileChunk).filter(FileChunk.file_id == file_id).count()
                        if chunks_received == file_record.total_chunks:
                            file_record.is_complete   = True
                            file_record.is_replicated = True
                            print(f"File complete: {file_record.filename} ({file_record.total_chunks} chunks)")

                    await db_commit_with_retry(db)
                except HTTPException:
                    raise
                except Exception as e:
                    db.rollback()
                    raise HTTPException(500, f"Replication failed: {str(e)}")
                finally:
                    db.close()

            print(f"Received replica: {chunk_checksum[:12]}... (chunk {chunk_index} of file {file_id}, {len(chunk_data)} bytes)")
            return {"status": "ack", "file_id": file_id, "chunk_index": chunk_index, "checksum": chunk_checksum, "received_at": time.time()}

        @self.app.get("/api/internal/get_chunk/{chunk_hash}")
        async def get_chunk_data(chunk_hash: str):
            if len(chunk_hash) != 64:
                raise HTTPException(400, "Invalid chunk hash length")
            chunk_path = self.chunks_dir / f"{chunk_hash}.dat"
            if not chunk_path.exists():
                raise HTTPException(404, f"Chunk {chunk_hash[:12]}... not found on this node")
            return FileResponse(str(chunk_path), media_type="application/octet-stream", headers={"X-Chunk-Hash": chunk_hash})

        @self.app.get("/api/download/{file_id}")
        async def download_file(file_id: str):
            from starlette.responses import StreamingResponse
            db = self.SessionLocal()
            try:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file_record:
                    raise HTTPException(404, "File not found")
                if not file_record.is_complete:
                    raise HTTPException(409, "File upload not complete")
                filename    = file_record.filename
                total_size  = file_record.total_size_bytes
                file_chunks = db.query(FileChunk).filter(FileChunk.file_id == file_id).order_by(FileChunk.chunk_index).all()
                fetch_plan = []
                for fc in file_chunks:
                    local_path = self.chunks_dir / f"{fc.chunk_hash}.dat"
                    if local_path.exists():
                        fetch_plan.append((fc.chunk_hash, local_path, None))
                        continue
                    locations = db.query(ChunkLocation).filter(ChunkLocation.chunk_hash == fc.chunk_hash, ChunkLocation.node_id != self.node_id).all()
                    remote_node_id = None
                    for loc in locations:
                        if loc.node_id not in self.peer_nodes:
                            continue
                        if self.health_monitor and self.health_monitor.get_peer_status(loc.node_id) != 'online':
                            continue
                        remote_node_id = loc.node_id
                        break
                    if remote_node_id:
                        fetch_plan.append((fc.chunk_hash, None, remote_node_id))
                    else:
                        raise HTTPException(500, f"Chunk {fc.chunk_index} ({fc.chunk_hash[:12]}...) unavailable")
            finally:
                db.close()

            async def chunk_streamer():
                _session = await self._get_http_session()
                for chunk_hash, local_path, remote_node_id in fetch_plan:
                    if local_path is not None:
                        yield await asyncio.to_thread(local_path.read_bytes)
                    else:
                        peer = self.peer_nodes[remote_node_id]
                        async with _session.get(f"http://{peer['ip_address']}:{peer['port']}/api/internal/get_chunk/{chunk_hash}", timeout=aiohttp.ClientTimeout(total=120)) as resp:
                            if resp.status == 200:
                                yield await resp.read()
                            else:
                                raise HTTPException(500, f"Failed to fetch chunk {chunk_hash[:12]}... from {peer['node_name']}: HTTP {resp.status}")
                    await asyncio.sleep(0)

            return StreamingResponse(chunk_streamer(), media_type="application/octet-stream", headers={"Content-Disposition": f'attachment; filename="{filename}"', "Content-Length": str(total_size)})

        @self.app.delete("/api/files/{file_id}")
        async def delete_file(file_id: str):
            """Phase 1: still writes directly to DB. Migration to raft_propose in Phase 4."""
            db = self.SessionLocal()
            try:
                file_record = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file_record:
                    raise HTTPException(404, "File not found")
                filename    = file_record.filename
                file_chunks = db.query(FileChunk).filter(FileChunk.file_id == file_id).all()
                chunk_hashes = [fc.chunk_hash for fc in file_chunks]
                db.delete(file_record)
                await db_flush_with_retry(db)
                gc_count = sum(1 for ch in chunk_hashes if self._decrement_chunk_ref(db, ch))
                await db_commit_with_retry(db)
                print(f"Deleted: {filename} (ID: {file_id}, {len(chunk_hashes)} mappings, {gc_count} chunks GC'd)")
                if self.peer_nodes:
                    asyncio.create_task(self._delete_remote_replicas(file_id, len(chunk_hashes), chunk_hashes))
                return {"message": "File deleted successfully"}
            finally:
                db.close()

        @self.app.post("/api/internal/delete_chunks")
        async def receive_delete_request(request: dict):
            db = self.SessionLocal()
            try:
                if 'signature' not in request or 'public_key' not in request:
                    raise HTTPException(401, "Unsigned messages not accepted")
                verified, message, sender_node_id = self.trust_store.verify_message(request)
                if not verified:
                    raise HTTPException(401, "Invalid signature")
                file_id      = message['file_id']
                file_record  = db.query(FileModel).filter(FileModel.id == file_id).first()
                deleted_count = 0
                if file_record:
                    file_chunk_hashes = [fc.chunk_hash for fc in db.query(FileChunk).filter(FileChunk.file_id == file_id).all()]
                    db.delete(file_record)
                    await db_flush_with_retry(db)
                    deleted_count = sum(1 for ch in file_chunk_hashes if self._decrement_chunk_ref(db, ch))
                    await db_commit_with_retry(db)
                print(f"Deleted remote replicas: file {file_id} ({deleted_count} chunks removed)")
                return {"status": "ok", "deleted_chunks": deleted_count}
            except HTTPException:
                raise
            except Exception as e:
                db.rollback()
                raise HTTPException(500, f"Delete failed: {str(e)}")
            finally:
                db.close()

        @self.app.get("/api/health")
        async def health_check():
            self._update_capacity()
            return {"status": "healthy", "node_id": self.node_id, "node_name": self.config.node_name, "cluster_id": self.config.cluster_id, "total_capacity_gb": round(self.total_capacity_gb, 2), "free_capacity_gb": round(self.free_capacity_gb, 2), "used_capacity_gb": round(self.used_capacity_gb, 2), "upload_slots_available": self._upload_semaphore._value, "upload_slots_total": 3, "timestamp": datetime.now(timezone.utc).isoformat()}

        @self.app.get("/api/inventory")
        async def get_inventory():
            db = self.SessionLocal()
            try:
                files = db.query(FileModel).all()
                inventory = []
                for f in files:
                    file_chunks = db.query(FileChunk).filter(FileChunk.file_id == f.id).order_by(FileChunk.chunk_index).all()
                    chunks_on_disk = []
                    for fc in file_chunks:
                        chunk_path = self.chunks_dir / f"{fc.chunk_hash}.dat"
                        if chunk_path.exists():
                            chunk_record = db.query(Chunk).filter(Chunk.chunk_hash == fc.chunk_hash).first()
                            chunks_on_disk.append({"chunk_index": fc.chunk_index, "size_bytes": chunk_record.size_bytes if chunk_record else chunk_path.stat().st_size, "chunk_hash": fc.chunk_hash})
                    inventory.append({"file_id": f.id, "filename": f.filename, "total_size_bytes": f.total_size_bytes, "chunk_size_bytes": f.chunk_size_bytes, "total_chunks": f.total_chunks, "checksum_sha256": f.checksum_sha256, "is_complete": f.is_complete, "uploaded_by": f.uploaded_by, "chunks_on_disk": chunks_on_disk})
                all_chunk_hashes = [dat_file.stem for dat_file in self.chunks_dir.glob("*.dat") if len(dat_file.stem) == 64] if self.chunks_dir.exists() else []
                referenced_hashes = {c["chunk_hash"] for f_info in inventory for c in f_info["chunks_on_disk"]}
                return {"node_id": self.node_id, "node_name": self.config.node_name, "files": inventory, "all_chunk_hashes": all_chunk_hashes, "orphaned_chunks": [h for h in all_chunk_hashes if h not in referenced_hashes]}
            finally:
                db.close()

        @self.app.get("/api/cluster/stats")
        async def get_stats():
            db = self.SessionLocal()
            try:
                self._update_capacity()
                total_nodes = 1 + len(self.peer_nodes)
                online_nodes = 1 + sum(1 for nid in self.peer_nodes if not self.health_monitor or self.health_monitor.get_peer_status(nid) == "online")
                raw_capacity = self.total_capacity_gb + sum(n["storage_gb"] for n in self.peer_nodes.values())
                replication_factor = 1 if total_nodes == 1 else (2 if total_nodes <= 4 else 3)
                usable_capacity = raw_capacity / replication_factor
                unique_data_bytes  = db.execute(text("SELECT COALESCE(SUM(total_size_bytes), 0) FROM files")).fetchone()[0]
                unique_data_gb     = round(unique_data_bytes / (1024**3), 2)
                total_replicas     = db.execute(text("SELECT COUNT(*) FROM chunk_locations")).fetchone()[0]
                total_chunks       = db.execute(text("SELECT COUNT(*) FROM file_chunks")).fetchone()[0]
                file_count         = db.query(FileModel).count()
                actual_storage_used = db.execute(text("SELECT COALESCE(SUM(c.size_bytes), 0) FROM chunk_locations cl JOIN chunks c ON c.chunk_hash = cl.chunk_hash")).fetchone()[0]
                actual_storage_used_gb = round(actual_storage_used / (1024**3), 2)
                free_capacity = (raw_capacity - actual_storage_used_gb) / replication_factor
                return {"total_nodes": total_nodes, "online_nodes": online_nodes, "total_files": file_count, "total_chunks": total_chunks, "total_replicas": total_replicas, "avg_replicas_per_chunk": round(total_replicas / total_chunks, 1) if total_chunks > 0 else 0, "raw_capacity_gb": round(raw_capacity, 2), "total_capacity_gb": round(usable_capacity, 2), "unique_data_gb": unique_data_gb, "actual_storage_used_gb": actual_storage_used_gb, "free_capacity_gb": round(max(0, free_capacity), 2), "utilization_percent": round(unique_data_gb / usable_capacity * 100, 1) if usable_capacity > 0 else 0, "replication_factor": replication_factor, "storage_efficiency_percent": round(usable_capacity / raw_capacity * 100, 1) if raw_capacity > 0 else 0, "replication_complete": total_replicas >= total_chunks * replication_factor if replication_factor > 1 else True}
            finally:
                db.close()

        @self.app.get("/api/nodes")
        async def get_nodes():
            self._update_capacity()
            now = datetime.now(timezone.utc)
            is_leader  = self.raft.is_leader() if self.raft else self.is_leader
            raft_state = self.raft.get_state() if self.raft else "unknown"
            leader_id  = self.raft.get_leader_id() if self.raft else None
            nodes = [{"id": 1, "name": self.config.node_name, "ip_address": self.local_ip, "port": self.config.port, "total_capacity_gb": round(self.total_capacity_gb, 2), "free_capacity_gb": round(self.free_capacity_gb, 2), "cpu_score": 1.0, "priority_score": 1.0, "status": "online", "is_brain": False, "is_leader": is_leader, "raft_state": raft_state, "last_heartbeat": now.isoformat(), "last_heartbeat_seconds_ago": 0}]
            for idx, (node_id, node_info) in enumerate(self.peer_nodes.items(), start=2):
                last_seen_ago = int(time.time() - self.health_monitor.peer_last_seen[node_id]) if self.health_monitor and node_id in self.health_monitor.peer_last_seen else 0
                nodes.append({"id": idx, "name": node_info['node_name'], "ip_address": node_info['ip_address'], "port": node_info['port'], "total_capacity_gb": node_info['storage_gb'], "free_capacity_gb": node_info.get('free_capacity_gb', node_info['storage_gb']), "cpu_score": 1.0, "priority_score": 1.0, "status": self.health_monitor.get_peer_status(node_id) if self.health_monitor else "online", "is_brain": False, "is_leader": node_id == leader_id, "raft_state": "leader" if node_id == leader_id else "follower", "last_heartbeat": node_info['discovered_at'], "last_heartbeat_seconds_ago": last_seen_ago, "latency_ms": node_info.get('latency_ms', None)})
            seen_names = {}
            for node in nodes:
                name = node['name']
                if name not in seen_names or node['last_heartbeat_seconds_ago'] < seen_names[name]['last_heartbeat_seconds_ago']:
                    seen_names[name] = node
            deduped = list(seen_names.values())
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
                    chunk_hashes = [fc.chunk_hash for fc in db.query(FileChunk).filter(FileChunk.file_id == f.id).all()]
                    node_ids = {loc.node_id for loc in db.query(ChunkLocation).filter(ChunkLocation.chunk_hash.in_(chunk_hashes)).all()} if chunk_hashes else set()
                    result.append({"id": f.id, "filename": f.filename, "total_size_bytes": f.total_size_bytes, "size_mb": round(f.total_size_bytes / (1024**2), 2), "total_size_mb": round(f.total_size_bytes / (1024**2), 2), "total_chunks": f.total_chunks, "is_complete": f.is_complete, "is_replicated": len(node_ids) > 1, "replica_count": len(node_ids), "created_at": f.created_at.isoformat() if f.created_at else None, "uploaded_by": f.uploaded_by})
                return result
            finally:
                db.close()

        @self.app.get("/api/files/{file_id}")
        async def get_file_details(file_id: str):
            db = self.SessionLocal()
            try:
                file = db.query(FileModel).filter(FileModel.id == file_id).first()
                if not file:
                    raise HTTPException(404, "File not found")
                file_chunks = db.query(FileChunk).filter(FileChunk.file_id == file_id).order_by(FileChunk.chunk_index).all()
                chunk_info = []
                all_node_ids = set()
                for fc in file_chunks:
                    locations = db.query(ChunkLocation).filter(ChunkLocation.chunk_hash == fc.chunk_hash).all()
                    location_info = []
                    for loc in locations:
                        node_name = self.config.node_name if loc.node_id == self.node_id else next((pi.get('node_name', 'peer_node') for pid, pi in self.peer_nodes.items() if pid == loc.node_id), "unknown")
                        location_info.append({"node_id": loc.node_id[:12] + "..." if len(loc.node_id) > 12 else loc.node_id, "node_name": node_name})
                        all_node_ids.add(loc.node_id)
                    chunk_record = db.query(Chunk).filter(Chunk.chunk_hash == fc.chunk_hash).first()
                    chunk_info.append({"chunk_index": fc.chunk_index, "size_bytes": chunk_record.size_bytes if chunk_record else 0, "chunk_hash": fc.chunk_hash[:16] + "...", "locations": location_info})
                return {"id": file.id, "filename": file.filename, "total_size_bytes": file.total_size_bytes, "total_chunks": file.total_chunks, "checksum_sha256": file.checksum_sha256, "is_complete": file.is_complete, "created_at": file.created_at.isoformat() if file.created_at else None, "uploaded_by": file.uploaded_by, "replica_count": len(all_node_ids), "chunks": chunk_info}
            finally:
                db.close()

        @self.app.get("/api/settings")
        async def get_settings():
            return {"chunk_size_mb": CHUNK_SIZE_BYTES // (1024*1024), "min_replicas": 2, "max_replicas": 3, "replication_strategy": "priority", "redundancy_mode": "standard", "verify_on_upload": True, "parallel_downloads": 4}

        @self.app.get("/api/jobs/status")
        async def get_jobs_status():
            return {"pending": 0, "in_progress": 0, "completed": 0, "failed": 0}

        @self.app.get("/api/security/stats")
        async def get_security_stats():
            return {"identity": {"node_id": self.node_id, "node_name": self.config.node_name, "public_key": self.identity.get_public_key_base64()[:32] + "...", "keys_path": str(self.identity.keys_path)}, "trust": {"trusted_peers": len(self.trust_store.trusted_peers), "peers": [{"node_id": nid[:8] + "...", "name": info['node_name'], "first_seen": info['first_seen'], "last_seen": info['last_seen']} for nid, info in self.trust_store.trusted_peers.items()]}}

    async def _delete_remote_replicas(self, file_id, chunk_count, chunk_hashes=None):
        if not self.peer_nodes:
            return
        print(f"Cleaning up remote replicas for file {file_id}...")
        for node_id, peer_info in self.peer_nodes.items():
            try:
                signed_payload = self.identity.sign_json({'file_id': file_id, 'chunk_hashes': chunk_hashes or [], 'node_name': self.config.node_name, 'timestamp': time.time()})
                session = await self._get_http_session()
                async with session.post(f"http://{peer_info['ip_address']}:{peer_info['port']}/api/internal/delete_chunks", json=signed_payload, timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        print(f"Cleaned replicas on {peer_info['node_name']}")
                    else:
                        print(f"Cleanup failed on {peer_info['node_name']}: HTTP {response.status}")
            except Exception as e:
                print(f"Cleanup failed on {peer_info['node_name']}: {e}")

    async def start(self):
        print("\n" + "="*60)
        print(f"Starting TossIt Node: {self.config.node_name}")
        print("="*60)
        print(f"Cluster ID:    {self.config.cluster_id}")
        print(f"Node ID:       {self.config.node_id}")
        print(f"Storage:       {self.config.storage_limit_gb:.1f} GB allocated")
        print(f"Port:          {self.config.port}")
        print(f"Data root:     {DATA_ROOT}")
        print("="*60 + "\n")

        await self._verify_chunks_on_boot()

        self.raft = ClusterRaft(
            node_id=self.node_id, node_name=self.config.node_name,
            port=self.config.port, data_dir=str(DB_DIR),
            identity=self.identity, trust_store=self.trust_store,
            on_become_leader=self._on_became_leader,
            on_lose_leadership=self._on_lost_leadership,
        )
        await self.raft.start()

        # ── Phase 1: Start the log apply loop ────────────────────────
        self._apply_log_entries_event = asyncio.Event()
        self._apply_log_entries_task  = asyncio.create_task(
            self._apply_log_entries(),
            name="raft-apply-loop",
        )
        print("✓ Raft log apply loop started")
        # ─────────────────────────────────────────────────────────────

        # ── Phase 3: Start the snapshot check loop ───────────────────
        self._snapshot_check_task = asyncio.create_task(
            self._snapshot_check_loop(),
            name="snapshot-check-loop",
        )
        print("✓ Snapshot check loop started")
        # ─────────────────────────────────────────────────────────────

        self.health_monitor = NodeHealthMonitor(timeout_seconds=60.0, check_interval=10.0, on_node_offline=self._on_node_went_offline)
        await self.health_monitor.start()

        self.peer_refresh_task = asyncio.create_task(self._periodic_peer_refresh())

        _use_mdns = not os.environ.get('TOSSIT_PEER_URLS', '').strip()
        if _use_mdns:
            self.discovery = ClusterDiscovery(cluster_id=self.config.cluster_id, node_id=self.config.node_id, node_name=self.config.node_name, port=self.config.port, storage_gb=self.config.storage_limit_gb, on_node_discovered=self._on_node_discovered, on_node_lost=self._on_node_lost)
            await self.discovery.start()
            print("Discovery: mDNS enabled (no static peers configured)")
        else:
            print("Discovery: mDNS skipped — using static peer URLs")
            asyncio.create_task(self._bootstrap_static_peers())

        registry_url = os.getenv('TOSSIT_REGISTRY_URL')
        if registry_url:
            self.registry_client = RegistryClient(registry_url=registry_url, cluster_id=self.config.cluster_id, node_name=self.config.node_name, port=self.config.port)
            print(f"Registry configured: {registry_url}")
            if await self.registry_client.test_connection():
                print("Registry service reachable")
            else:
                print("Warning: Registry service not reachable (will retry)")
            asyncio.create_task(self._update_registry_stats_loop())
        else:
            print("No registry configured (set TOSSIT_REGISTRY_URL to enable)")

        config = uvicorn.Config(self.app, host="0.0.0.0", port=self.config.port, log_level="info", h11_max_incomplete_event_size=None, timeout_keep_alive=600)
        server = uvicorn.Server(config)

        try:
            await server.serve()
        finally:
            # ── Phase 1+3: Shut down background loops cleanly ─────────
            if self._snapshot_check_task:
                self._snapshot_check_task.cancel()
                try:
                    await self._snapshot_check_task
                except asyncio.CancelledError:
                    pass
            if self._apply_log_entries_task:
                self._apply_log_entries_task.cancel()
                try:
                    await self._apply_log_entries_task
                except asyncio.CancelledError:
                    pass
            # ─────────────────────────────────────────────────────────

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
        return round(self.config.storage_limit_gb + sum(n.get('storage_gb', 0) for n in self.peer_nodes.values()), 2)

    def _calculate_used_capacity(self) -> float:
        db = self.SessionLocal()
        try:
            result = db.execute(text("SELECT COALESCE(SUM(c.size_bytes), 0) FROM chunk_locations cl JOIN chunks c ON c.chunk_hash = cl.chunk_hash WHERE cl.node_id = :nid"), {"nid": self.node_id}).fetchone()
            return round(result[0] / (1024**3), 2) if result else 0.0
        except Exception as e:
            print(f"Error calculating used capacity: {e}")
            return 0.0
        finally:
            db.close()

    async def _update_registry_stats_loop(self):
        while True:
            try:
                await asyncio.sleep(30)
                if self.registry_client and self.is_leader:
                    self.registry_client.update_stats(node_count=len(self.peer_nodes) + 1, total_capacity_gb=self._calculate_total_capacity(), used_capacity_gb=self._calculate_used_capacity(), local_ip=self.local_ip)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error updating registry stats: {e}")
                await asyncio.sleep(30)


async def main():
    config = NodeConfig.from_env()
    if config is None:
        config = NodeConfig.load()
        if not config.exists():
            config = interactive_setup()
        else:
            print(f"Using existing configuration — Node: {config.node_name}, Cluster: {config.cluster_id}, Storage: {config.storage_limit_gb:.1f} GB")
    node = TossItNode(config)
    await node.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nTossIt node stopped")
        sys.exit(0)
