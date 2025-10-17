"""
TossIt Database Models
Defines the schema for nodes, files, chunks, replicas, jobs, and audit logs
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey, Text, Enum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
import enum

Base = declarative_base()

class NodeStatus(enum.Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"

class JobStatus(enum.Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

class JobType(enum.Enum):
    REPLICATION = "replication"
    VERIFICATION = "verification"
    REBALANCE = "rebalance"
    DELETION = "deletion"

class Node(Base):
    __tablename__ = 'nodes'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(100), unique=True, nullable=False)
    ip_address = Column(String(50), nullable=False)
    port = Column(Integer, default=8080)
    
    # Capacity metrics
    total_capacity_gb = Column(Float, nullable=False)
    free_capacity_gb = Column(Float, nullable=False)
    
    # Performance metrics
    cpu_score = Column(Float, default=1.0)  # Relative performance score
    network_speed_mbps = Column(Float, default=100.0)
    avg_uptime_percent = Column(Float, default=100.0)
    
    # Priority calculation
    priority_score = Column(Float, default=1.0)  # Computed from above metrics
    
    # Status
    status = Column(Enum(NodeStatus), default=NodeStatus.ONLINE)
    last_heartbeat = Column(DateTime, default=datetime.utcnow)
    
    # Metadata
    is_brain = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    replicas = relationship("Replica", back_populates="node", cascade="all, delete-orphan")

class File(Base):
    __tablename__ = 'files'
    
    id = Column(Integer, primary_key=True)
    filename = Column(String(500), nullable=False)
    original_path = Column(String(1000))
    
    # File metadata
    total_size_bytes = Column(Integer, nullable=False)
    chunk_size_bytes = Column(Integer, default=67108864)  # 64 MB default
    total_chunks = Column(Integer, nullable=False)
    
    # File info
    mime_type = Column(String(100))
    checksum_sha256 = Column(String(64))  # Full file hash
    
    # Status
    is_complete = Column(Boolean, default=False)  # All chunks uploaded
    is_replicated = Column(Boolean, default=False)  # All chunks have min replicas
    
    # Metadata
    uploaded_by = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_accessed = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    chunks = relationship("Chunk", back_populates="file", cascade="all, delete-orphan")

class Chunk(Base):
    __tablename__ = 'chunks'
    
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('files.id'), nullable=False)
    chunk_index = Column(Integer, nullable=False)  # 0-based position in file
    
    # Chunk data
    size_bytes = Column(Integer, nullable=False)
    checksum_sha256 = Column(String(64), nullable=False)  # Chunk hash for verification
    
    # Primary replica tracking
    primary_node_id = Column(Integer, ForeignKey('nodes.id'))
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    file = relationship("File", back_populates="chunks")
    replicas = relationship("Replica", back_populates="chunk", cascade="all, delete-orphan")

class Replica(Base):
    __tablename__ = 'replicas'
    
    id = Column(Integer, primary_key=True)
    chunk_id = Column(Integer, ForeignKey('chunks.id'), nullable=False)
    node_id = Column(Integer, ForeignKey('nodes.id'), nullable=False)
    
    # Replica metadata
    is_primary = Column(Boolean, default=False)
    local_path = Column(String(1000))  # Path on the node's filesystem
    
    # Verification
    last_verified = Column(DateTime)
    verification_status = Column(String(20), default="unverified")  # unverified, valid, corrupted
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    chunk = relationship("Chunk", back_populates="replicas")
    node = relationship("Node", back_populates="replicas")

class Job(Base):
    __tablename__ = 'jobs'
    
    id = Column(Integer, primary_key=True)
    job_type = Column(Enum(JobType), nullable=False)
    status = Column(Enum(JobStatus), default=JobStatus.PENDING)
    
    # Job targets
    chunk_id = Column(Integer, ForeignKey('chunks.id'))
    source_node_id = Column(Integer, ForeignKey('nodes.id'))
    target_node_id = Column(Integer, ForeignKey('nodes.id'))
    
    # Progress tracking
    progress_percent = Column(Float, default=0.0)
    error_message = Column(Text)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    
    # Priority
    priority = Column(Integer, default=5)  # 1-10, higher = more urgent
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
class AuditLog(Base):
    __tablename__ = 'audit_log'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Event details
    event_type = Column(String(50), nullable=False)  # upload, download, delete, replicate, etc.
    node_id = Column(Integer, ForeignKey('nodes.id'))
    file_id = Column(Integer, ForeignKey('files.id'))
    chunk_id = Column(Integer, ForeignKey('chunks.id'))
    
    # Event data
    details = Column(Text)  # JSON string with additional context
    user = Column(String(100))
    
    # Result
    success = Column(Boolean, default=True)
    error_message = Column(Text)


def init_database(db_path='tossit.db'):
    """Initialize the database and create all tables"""
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    return engine