"""
ACID Consistency Validation for TossIt
Ensures database invariants are maintained at all times
"""

from sqlalchemy.orm import Session
from typing import List, Dict, Tuple, Optional
from datetime import datetime
from models import Node, FileModel as FileModel, Chunk, Replica, NodeStatus


class ConsistencyValidator:
    """Validates and enforces database consistency invariants"""
    
    def __init__(self):
        self.violations = []
        self.warnings = []
    
    def validate_all(self, db: Session) -> Tuple[bool, List[str], List[str]]:
        """
        Run all consistency checks
        
        Returns:
            (is_consistent, violations, warnings)
        """
        self.violations = []
        self.warnings = []
        
        print("🔍 Running ACID consistency validation...")
        
        # Run all validation checks
        self._validate_node_capacity(db)
        self._validate_file_completeness(db)
        self._validate_chunk_replicas(db)
        self._validate_orphaned_data(db)
        self._validate_space_accounting(db)
        
        is_consistent = len(self.violations) == 0
        
        if is_consistent:
            print("✅ Database is ACID consistent")
        else:
            print(f"❌ Found {len(self.violations)} consistency violations")
        
        if self.warnings:
            print(f"⚠️  Found {len(self.warnings)} warnings")
        
        return is_consistent, self.violations, self.warnings
    
    def _validate_node_capacity(self, db: Session):
        """
        Invariant: node.total_capacity = node.free_capacity + node.used_capacity
        
        Verify that tracked capacity matches actual chunk storage
        """
        nodes = db.query(Node).all()
        
        for node in nodes:
            # Calculate actual used space from chunks
            chunks = db.query(Chunk).filter(Chunk.primary_node_id == node.id).all()
            actual_used_gb = sum(c.size_bytes for c in chunks) / (1024 ** 3)
            
            # Calculate tracked used space
            tracked_used_gb = node.total_capacity_gb - node.free_capacity_gb
            
            # Allow 10 MB tolerance for rounding
            tolerance_gb = 0.01
            difference = abs(actual_used_gb - tracked_used_gb)
            
            if difference > tolerance_gb:
                self.violations.append(
                    f"Node '{node.name}' capacity mismatch: "
                    f"tracked={tracked_used_gb:.3f} GB, actual={actual_used_gb:.3f} GB "
                    f"(difference: {difference:.3f} GB)"
                )
            
            # Check for negative free space
            if node.free_capacity_gb < 0:
                self.violations.append(
                    f"Node '{node.name}' has NEGATIVE free capacity: {node.free_capacity_gb:.3f} GB"
                )
            
            # Check for over-capacity
            if tracked_used_gb > node.total_capacity_gb:
                self.violations.append(
                    f"Node '{node.name}' EXCEEDS total capacity: "
                    f"used={tracked_used_gb:.3f} GB > total={node.total_capacity_gb:.3f} GB"
                )
    
    def _validate_file_completeness(self, db: Session):
        """
        Invariant: file.is_complete=True => has all chunks
        
        Files marked complete must have all their chunks
        """
        complete_files = db.query(FileModel).filter(FileModel.is_complete == True).all()
        
        for file_record in complete_files:
            chunk_count = db.query(Chunk).filter(Chunk.file_id == file_record.id).count()
            
            if chunk_count != file_record.total_chunks:
                self.violations.append(
                    f"FileModel '{file_record.filename}' marked complete but has "
                    f"{chunk_count}/{file_record.total_chunks} chunks"
                )
            
            # Check for chunks with no replicas
            chunks_without_replicas = db.query(Chunk).filter(
                Chunk.file_id == file_record.id
            ).all()
            
            for chunk in chunks_without_replicas:
                replica_count = db.query(Replica).filter(Replica.chunk_id == chunk.id).count()
                if replica_count == 0:
                    self.violations.append(
                        f"Chunk {chunk.id} (file '{file_record.filename}') has NO replicas"
                    )
    
    def _validate_chunk_replicas(self, db: Session):
        """
        Invariant: Every chunk should have at least one replica (primary)
        
        Warn if chunks don't meet minimum replica requirements
        """
        from tossit_settings import get_settings
        settings = get_settings(db)
        min_replicas = settings.min_replicas
        
        all_chunks = db.query(Chunk).all()
        
        for chunk in all_chunks:
            replicas = db.query(Replica).filter(Replica.chunk_id == chunk.id).all()
            replica_count = len(replicas)
            
            if replica_count == 0:
                self.violations.append(
                    f"Chunk {chunk.id} has NO replicas (not even primary!)"
                )
            elif replica_count < min_replicas:
                # This is a warning, not a violation (degraded redundancy is acceptable)
                self.warnings.append(
                    f"Chunk {chunk.id} has {replica_count}/{min_replicas} replicas (degraded redundancy)"
                )
            
            # Verify primary replica exists
            primary_replicas = [r for r in replicas if r.is_primary]
            if len(primary_replicas) == 0:
                self.violations.append(
                    f"Chunk {chunk.id} has no PRIMARY replica"
                )
            elif len(primary_replicas) > 1:
                self.violations.append(
                    f"Chunk {chunk.id} has {len(primary_replicas)} PRIMARY replicas (should be 1)"
                )
    
    def _validate_orphaned_data(self, db: Session):
        """
        Detect orphaned database records that reference non-existent entities
        """
        # Orphaned chunks (file doesn't exist)
        orphaned_chunks = db.query(Chunk).outerjoin(
            FileModel, Chunk.file_id == FileModel.id
        ).filter(FileModel.id == None).all()
        
        if orphaned_chunks:
            self.violations.append(
                f"Found {len(orphaned_chunks)} orphaned chunks (file deleted but chunks remain)"
            )
        
        # Orphaned replicas (chunk doesn't exist)
        orphaned_replicas = db.query(Replica).outerjoin(
            Chunk, Replica.chunk_id == Chunk.id
        ).filter(Chunk.id == None).all()
        
        if orphaned_replicas:
            self.violations.append(
                f"Found {len(orphaned_replicas)} orphaned replicas (chunk deleted but replicas remain)"
            )
        
        # Incomplete files older than 1 hour (likely failed uploads)
        from datetime import timedelta
        one_hour_ago = datetime.utcnow() - timedelta(hours=1)
        stale_incomplete = db.query(FileModel).filter(
            FileModel.is_complete == False,
            FileModel.created_at < one_hour_ago
        ).all()
        
        if stale_incomplete:
            self.warnings.append(
                f"Found {len(stale_incomplete)} stale incomplete files (>1 hour old)"
            )
    
    def _validate_space_accounting(self, db: Session):
        """
        Validate that space reservations and actual usage match across cluster
        """
        nodes = db.query(Node).all()
        
        total_capacity = sum(n.total_capacity_gb for n in nodes)
        total_free = sum(n.free_capacity_gb for n in nodes)
        total_used = total_capacity - total_free
        
        # Calculate actual space from chunks (counting primary only, not replicas)
        all_chunks = db.query(Chunk).all()
        actual_used = sum(c.size_bytes for c in all_chunks) / (1024 ** 3)
        
        difference = abs(total_used - actual_used)
        
        if difference > 0.1:  # 100 MB tolerance
            self.warnings.append(
                f"Cluster space accounting mismatch: "
                f"tracked={total_used:.3f} GB, actual={actual_used:.3f} GB "
                f"(difference: {difference:.3f} GB)"
            )
    
    def repair_suggestions(self, db: Session) -> List[str]:
        """
        Generate suggestions for repairing consistency violations
        """
        suggestions = []
        
        # Run validation first
        self.validate_all(db)
        
        if not self.violations:
            return ["✅ No repairs needed - database is consistent"]
        
        # Analyze violations and suggest repairs
        for violation in self.violations:
            if "capacity mismatch" in violation.lower():
                suggestions.append(
                    "Recalculate node capacity from actual chunks:\n"
                    "  POST /api/nodes/recalculate-capacity"
                )
            
            if "marked complete but has" in violation.lower():
                suggestions.append(
                    "Mark incomplete files as incomplete:\n"
                    "  POST /api/files/fix-incomplete"
                )
            
            if "has no replicas" in violation.lower():
                suggestions.append(
                    "Clean up chunks without replicas:\n"
                    "  DELETE /api/chunks/orphaned"
                )
            
            if "orphaned" in violation.lower():
                suggestions.append(
                    "Clean up orphaned records:\n"
                    "  POST /api/cleanup/orphans"
                )
        
        # Remove duplicates
        return list(set(suggestions))


def validate_database_consistency(db: Session, auto_report: bool = True) -> bool:
    """
    Convenience function to validate database consistency
    
    Args:
        db: Database session
        auto_report: If True, print violations and warnings
    
    Returns:
        True if database is consistent, False otherwise
    """
    validator = ConsistencyValidator()
    is_consistent, violations, warnings = validator.validate_all(db)
    
    if auto_report:
        if violations:
            print("\n❌ CONSISTENCY VIOLATIONS:")
            for violation in violations:
                print(f"  • {violation}")
        
        if warnings:
            print("\n⚠️  WARNINGS:")
            for warning in warnings:
                print(f"  • {warning}")
        
        if not is_consistent:
            print("\n🔧 SUGGESTED REPAIRS:")
            suggestions = validator.repair_suggestions(db)
            for suggestion in suggestions:
                print(f"  {suggestion}")
    
    return is_consistent