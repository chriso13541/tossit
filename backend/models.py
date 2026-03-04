#!/usr/bin/env python3
"""
TossIt Data Models — Content-Addressed Chunk Storage

Architecture:
  Files = logical objects (sequential ID, user-facing)
  Chunks = content-addressed (SHA-256 hash IS the identity)
  FileChunks = ordered mapping from files to chunks
  ChunkLocations = which nodes have which chunks

Key principle: chunk identity is its content hash, globally unique.
Same data → same chunk_hash everywhere. Different data → different hash.
"""

from sqlalchemy import (
    Column, Integer, String, Float, Boolean, DateTime, Enum as SQLEnum,
    ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, relationship
from datetime import datetime, timezone
import enum


Base = declarative_base()


# ================================================================
#  Enums
# ================================================================

class NodeStatus(enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"


class JobStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class JobType(enum.Enum):
    REPLICATION = "replication"
    DELETION = "deletion"
    MIGRATION = "migration"


# ================================================================
#  Node — physical machine in the cluster
# ================================================================

class Node(Base):
    __tablename__ = 'nodes'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    ip_address = Column(String, nullable=False)
    port = Column(Integer, default=8000)
    total_capacity_gb = Column(Float, default=0.0)
    free_capacity_gb = Column(Float, default=0.0)
    cpu_score = Column(Float, default=1.0)
    network_speed_mbps = Column(Float, default=100.0)
    avg_uptime_percent = Column(Float, default=100.0)
    status = Column(SQLEnum(NodeStatus), default=NodeStatus.ONLINE)
    last_heartbeat = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ================================================================
#  File — logical file object (user-facing, sequential ID)
# ================================================================

class File(Base):
    __tablename__ = 'files'

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String, nullable=False)
    total_size_bytes = Column(Integer, nullable=False)
    chunk_size_bytes = Column(Integer, default=64 * 1024 * 1024)
    total_chunks = Column(Integer, nullable=False)
    checksum_sha256 = Column(String, nullable=False)
    is_complete = Column(Boolean, default=False)
    is_replicated = Column(Boolean, default=False)
    uploaded_by = Column(String, default='unknown')
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationship to ordered chunks
    chunks = relationship("FileChunk", back_populates="file",
                          order_by="FileChunk.chunk_index",
                          cascade="all, delete-orphan")


# ================================================================
#  FileChunk — ordered mapping from file to content-addressed chunks
#
#  This is the bridge between logical files and physical chunks.
#  chunk_hash is the SHA-256 of the chunk bytes — the global identity.
#  Multiple files can reference the same chunk_hash (deduplication).
# ================================================================

class FileChunk(Base):
    __tablename__ = 'file_chunks'

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(Integer, ForeignKey('files.id', ondelete='CASCADE'), nullable=False)
    chunk_index = Column(Integer, nullable=False)  # Position in file (0, 1, 2...)
    chunk_hash = Column(String(64), nullable=False)  # SHA-256 content hash
    size_bytes = Column(Integer, nullable=False)

    file = relationship("File", back_populates="chunks")

    __table_args__ = (
        UniqueConstraint('file_id', 'chunk_index', name='uq_file_chunk_index'),
        Index('ix_file_chunks_hash', 'chunk_hash'),
    )


# ================================================================
#  ChunkLocation — tracks which nodes store which chunks
#
#  chunk_hash references the content-addressed chunk.
#  node_id = 1 means "this node" (same convention as before).
#  A chunk is considered replicated if it appears on multiple node_ids.
# ================================================================

class ChunkLocation(Base):
    __tablename__ = 'chunk_locations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    chunk_hash = Column(String(64), nullable=False)
    node_id = Column(Integer, nullable=False)  # 1 = self

    __table_args__ = (
        UniqueConstraint('chunk_hash', 'node_id', name='uq_chunk_node'),
        Index('ix_chunk_locations_hash', 'chunk_hash'),
    )


# ================================================================
#  Job / AuditLog — placeholders for future use
# ================================================================

class Job(Base):
    __tablename__ = 'jobs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(SQLEnum(JobType), nullable=False)
    status = Column(SQLEnum(JobStatus), default=JobStatus.PENDING)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    completed_at = Column(DateTime, nullable=True)
    details = Column(String, nullable=True)


class AuditLog(Base):
    __tablename__ = 'audit_log'

    id = Column(Integer, primary_key=True, autoincrement=True)
    action = Column(String, nullable=False)
    details = Column(String, nullable=True)
    timestamp = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# Legacy aliases — only used for clean import compatibility
# New code should use FileChunk and ChunkLocation directly
Chunk = FileChunk
Replica = ChunkLocation
