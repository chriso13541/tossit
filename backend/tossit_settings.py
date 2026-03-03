"""
TossIt Cluster Settings
Manages cluster-wide configuration including replication strategy, chunk size, etc.
"""

from sqlalchemy import Column, Integer, String, Float, Boolean
from models import Base
from sqlalchemy.orm import Session

class ClusterSettings(Base):
    __tablename__ = 'cluster_settings'
    
    id = Column(Integer, primary_key=True)
    
    # Chunking settings
    chunk_size_mb = Column(Integer, default=64)  # Size of each chunk
    
    # Replication settings
    min_replicas = Column(Integer, default=2)  # Minimum copies of each chunk
    max_replicas = Column(Integer, default=3)  # Maximum copies of each chunk
    
    # Replication strategy
    replication_strategy = Column(String(50), default="priority")
    # Options: "priority" (best nodes first), "round_robin" (spread evenly), 
    #          "geographic" (future: by location), "random"
    
    # Striping settings
    enable_striping = Column(Boolean, default=True)  # Split files into chunks
    stripe_width = Column(Integer, default=1)  # Number of nodes to stripe across simultaneously
    
    # Performance settings
    parallel_uploads = Column(Boolean, default=False)  # Upload chunks in parallel
    parallel_downloads = Column(Boolean, default=True)  # Download chunks in parallel
    
    # Data integrity
    verify_on_upload = Column(Boolean, default=True)  # Verify checksums on upload
    auto_verify_interval_hours = Column(Integer, default=168)  # Weekly verification
    
    # Storage optimization
    compression_enabled = Column(Boolean, default=False)  # Compress chunks (future)
    deduplication_enabled = Column(Boolean, default=False)  # Deduplicate chunks (future)
    
    # Redundancy mode
    redundancy_mode = Column(String(50), default="standard")
    # Options: "minimal" (min_replicas only), "standard" (balanced), 
    #          "high" (max_replicas), "paranoid" (all nodes)


def get_settings(db: Session) -> ClusterSettings:
    """Get current cluster settings, creating defaults if none exist"""
    settings = db.query(ClusterSettings).first()
    
    if not settings:
        settings = ClusterSettings()
        db.add(settings)
        db.commit()
        db.refresh(settings)
    
    return settings


def update_settings(db: Session, settings_dict: dict) -> ClusterSettings:
    """Update cluster settings"""
    settings = get_settings(db)
    
    for key, value in settings_dict.items():
        if hasattr(settings, key):
            setattr(settings, key, value)
    
    db.commit()
    db.refresh(settings)
    
    return settings


# Replication strategy implementations
def select_replica_nodes_priority(db, primary_node, count, all_nodes):
    """Select nodes based on priority score"""
    from models import Node, NodeStatus
    available = [n for n in all_nodes 
                 if n.id != primary_node.id 
                 and n.status == NodeStatus.ONLINE
                 and n.free_capacity_gb > 0.1]
    
    # Sort by priority score (highest first)
    available.sort(key=lambda n: n.priority_score, reverse=True)
    
    return available[:count]


def select_replica_nodes_round_robin(db, primary_node, count, all_nodes):
    """Select nodes in round-robin fashion to balance load"""
    from models import Node, NodeStatus, Replica
    
    available = [n for n in all_nodes 
                 if n.id != primary_node.id 
                 and n.status == NodeStatus.ONLINE
                 and n.free_capacity_gb > 0.1]
    
    # Count existing replicas per node to balance
    replica_counts = {}
    for node in available:
        replica_counts[node.id] = db.query(Replica).filter_by(node_id=node.id).count()
    
    # Sort by replica count (fewest first)
    available.sort(key=lambda n: replica_counts.get(n.id, 0))
    
    return available[:count]


def select_replica_nodes_random(db, primary_node, count, all_nodes):
    """Select nodes randomly"""
    import random
    from models import NodeStatus
    
    available = [n for n in all_nodes 
                 if n.id != primary_node.id 
                 and n.status == NodeStatus.ONLINE
                 and n.free_capacity_gb > 0.1]
    
    random.shuffle(available)
    
    return available[:count]


def get_replica_nodes(db, primary_node, settings: ClusterSettings):
    """Get nodes for replication based on current settings"""
    from models import Node, NodeStatus
    
    all_nodes = db.query(Node).filter(Node.status == NodeStatus.ONLINE).all()
    
    # Determine replica count based on redundancy mode
    if settings.redundancy_mode == "minimal":
        replica_count = settings.min_replicas - 1  # -1 because primary counts as one
    elif settings.redundancy_mode == "high":
        replica_count = settings.max_replicas - 1
    elif settings.redundancy_mode == "paranoid":
        replica_count = len(all_nodes) - 1  # Replicate to all nodes
    else:  # standard
        replica_count = settings.min_replicas - 1
    
    # Select nodes based on strategy
    if settings.replication_strategy == "round_robin":
        return select_replica_nodes_round_robin(db, primary_node, replica_count, all_nodes)
    elif settings.replication_strategy == "random":
        return select_replica_nodes_random(db, primary_node, replica_count, all_nodes)
    else:  # priority (default)
        return select_replica_nodes_priority(db, primary_node, replica_count, all_nodes)