"""
TossIt Rebalancing System
Analyzes cluster distribution and migrates chunks to balance load across nodes
"""

from sqlalchemy.orm import Session
from models import Node, Chunk, Replica, Job, JobType, JobStatus, NodeStatus
from typing import List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class ClusterRebalancer:
    """Handles rebalancing of chunks across cluster nodes"""
    
    def __init__(self, db: Session):
        self.db = db
        
    def analyze_distribution(self) -> Dict:
        """Analyze current chunk distribution across nodes"""
        nodes = self.db.query(Node).filter(Node.status == NodeStatus.ONLINE).all()
        
        if len(nodes) == 0:
            return {"error": "No online nodes available"}
        
        # Count primary chunks per node
        distribution = {}
        total_chunks = 0
        
        for node in nodes:
            chunk_count = self.db.query(Chunk).filter(
                Chunk.primary_node_id == node.id
            ).count()
            
            distribution[node.id] = {
                "node_name": node.name,
                "chunk_count": chunk_count,
                "capacity_gb": node.total_capacity_gb,
                "free_gb": node.free_capacity_gb,
                "utilization_percent": ((node.total_capacity_gb - node.free_capacity_gb) / node.total_capacity_gb * 100) if node.total_capacity_gb > 0 else 0
            }
            total_chunks += chunk_count
        
        # Calculate ideal distribution
        ideal_per_node = total_chunks / len(nodes) if len(nodes) > 0 else 0
        
        # Calculate imbalance metrics
        imbalances = {}
        max_deviation = 0
        
        for node_id, info in distribution.items():
            deviation = info["chunk_count"] - ideal_per_node
            imbalances[node_id] = {
                "deviation": deviation,
                "deviation_percent": (abs(deviation) / ideal_per_node * 100) if ideal_per_node > 0 else 0
            }
            max_deviation = max(max_deviation, abs(deviation))
        
        # Calculate overall balance score (0-100, 100 = perfectly balanced)
        balance_score = 100 - min(100, (max_deviation / ideal_per_node * 100)) if ideal_per_node > 0 else 100
        
        return {
            "total_chunks": total_chunks,
            "total_nodes": len(nodes),
            "ideal_per_node": ideal_per_node,
            "distribution": distribution,
            "imbalances": imbalances,
            "balance_score": round(balance_score, 1),
            "max_deviation": max_deviation,
            "needs_rebalancing": balance_score < 80  # Rebalance if < 80% balanced
        }
    
    def create_rebalancing_plan(self, max_migrations: int = 50) -> List[Dict]:
        """Create a plan for rebalancing chunks across nodes"""
        analysis = self.analyze_distribution()
        
        if "error" in analysis:
            return []
        
        if not analysis["needs_rebalancing"]:
            logger.info("Cluster is already balanced (score: {})".format(analysis["balance_score"]))
            return []
        
        migrations = []
        
        # For severely imbalanced clusters (score < 20), use more aggressive thresholds
        if analysis["balance_score"] < 20:
            deviation_threshold = 2  # Move chunks even with small deviations
            move_percentage = 0.9    # Move 90% of excess chunks
            print(f"Severe imbalance detected (score: {analysis['balance_score']}), using aggressive rebalancing")
        else:
            deviation_threshold = 5  # Normal threshold
            move_percentage = 0.7    # Normal percentage
            print(f"Standard rebalancing (score: {analysis['balance_score']})")
        
        # Identify overloaded and underutilized nodes
        overloaded = []
        underutilized = []
        
        for node_id, imbalance in analysis["imbalances"].items():
            if imbalance["deviation"] > deviation_threshold:
                overloaded.append((node_id, analysis["distribution"][node_id], imbalance["deviation"]))
            elif imbalance["deviation"] < -deviation_threshold:
                underutilized.append((node_id, analysis["distribution"][node_id], abs(imbalance["deviation"])))
        
        # Sort by deviation (most imbalanced first)
        overloaded.sort(key=lambda x: x[2], reverse=True)
        underutilized.sort(key=lambda x: x[2], reverse=True)
        
        print(f"Rebalancing: {len(overloaded)} overloaded, {len(underutilized)} underutilized nodes")
        for node_id, info, deviation in overloaded:
            print(f"  Overloaded: {info['node_name']} has {info['chunk_count']} chunks (+{deviation:.1f} excess)")
        for node_id, info, space in underutilized:
            print(f"  Underutilized: {info['node_name']} has {info['chunk_count']} chunks (-{space:.1f} below ideal)")
        
        # Create migration plan
        migration_count = 0
        
        for source_node_id, source_info, source_deviation in overloaded:
            if migration_count >= max_migrations:
                break
            
            # Calculate how many chunks to move
            chunks_to_move = max(1, int(source_deviation * move_percentage))
            print(f"Planning to move {chunks_to_move} chunks from {source_info['node_name']}")
            
            # Get chunks from overloaded node
            chunks = self.db.query(Chunk).filter(
                Chunk.primary_node_id == source_node_id
            ).limit(chunks_to_move).all()
            
            for chunk in chunks:
                if migration_count >= max_migrations:
                    break
                
                # Find best target node (now more permissive)
                target = self._find_best_target_node(chunk, underutilized, migrations)
                
                if target:
                    target_node_id = target[0]
                    migrations.append({
                        "chunk_id": chunk.id,
                        "source_node_id": source_node_id,
                        "source_node_name": source_info["node_name"],
                        "target_node_id": target_node_id,
                        "target_node_name": target[1]["node_name"],
                        "chunk_size_mb": chunk.size_bytes / (1024**2)
                    })
                    migration_count += 1
                    print(f"  Migration {migration_count}: Chunk {chunk.id} from {source_info['node_name']} to {target[1]['node_name']}")
                else:
                    print(f"  Could not find target for chunk {chunk.id}")
        
        print(f"Created {len(migrations)} migrations")
        return migrations
    
    def _find_best_target_node(self, chunk: Chunk, underutilized: List, existing_migrations: List) -> Tuple:
        """Find the best target node for a chunk migration"""
        # Get all online nodes, not just underutilized ones
        all_nodes = self.db.query(Node).filter(Node.status == NodeStatus.ONLINE).all()
        
        # Count pending migrations to each node
        migration_counts = {}
        for migration in existing_migrations:
            target_id = migration["target_node_id"]
            migration_counts[target_id] = migration_counts.get(target_id, 0) + 1
        
        # Find node with most capacity and fewest pending migrations
        best_target = None
        best_score = -999999  # Allow negative scores for severely imbalanced clusters
        
        for node in all_nodes:
            # Don't migrate to node that already has a replica
            existing_replica = self.db.query(Replica).filter(
                Replica.chunk_id == chunk.id,
                Replica.node_id == node.id
            ).first()
            
            if existing_replica:
                continue
            
            # Don't migrate to the same node (source)
            if hasattr(chunk, 'primary_node_id') and chunk.primary_node_id == node.id:
                continue
            
            # Calculate current chunk count for this node
            current_chunks = self.db.query(Chunk).filter(
                Chunk.primary_node_id == node.id
            ).count()
            
            # Add pending migrations to current count
            pending = migration_counts.get(node.id, 0)
            projected_chunks = current_chunks + pending
            
            # Score based on: fewer chunks = better, more free space = better
            # Use a more generous scoring system for severely imbalanced clusters
            free_space_score = max(0, node.free_capacity_gb)  # Don't penalize negative space too much
            chunk_count_penalty = projected_chunks * 2  # Penalty for having many chunks
            
            score = free_space_score - chunk_count_penalty
            
            print(f"  Evaluating {node.name}: {current_chunks} chunks, {pending} pending, {node.free_capacity_gb:.1f}GB free -> score {score:.1f}")
            
            if score > best_score:
                best_score = score
                best_target = (node.id, {"node_name": node.name})
        
        if best_target:
            print(f"  Selected {best_target[1]['node_name']} with score {best_score:.1f}")
        else:
            print("  No suitable target found")
        
        return best_target
    
    def execute_rebalancing(self, migrations: List[Dict], priority: int = 5) -> int:
        """Execute rebalancing by creating migration jobs"""
        jobs_created = 0
        
        for migration in migrations:
            # Check if migration job already exists
            existing = self.db.query(Job).filter(
                Job.job_type == JobType.REBALANCE,
                Job.chunk_id == migration["chunk_id"],
                Job.target_node_id == migration["target_node_id"],
                Job.status.in_([JobStatus.PENDING, JobStatus.IN_PROGRESS])
            ).first()
            
            if existing:
                continue
            
            # Create migration job
            job = Job(
                job_type=JobType.REBALANCE,
                chunk_id=migration["chunk_id"],
                source_node_id=migration["source_node_id"],
                target_node_id=migration["target_node_id"],
                priority=priority,
                status=JobStatus.PENDING
            )
            self.db.add(job)
            jobs_created += 1
        
        self.db.commit()
        
        logger.info(f"Created {jobs_created} rebalancing jobs")
        
        return jobs_created
    
    def get_rebalancing_status(self) -> Dict:
        """Get current rebalancing progress"""
        pending = self.db.query(Job).filter(
            Job.job_type == JobType.REBALANCE,
            Job.status == JobStatus.PENDING
        ).count()
        
        in_progress = self.db.query(Job).filter(
            Job.job_type == JobType.REBALANCE,
            Job.status == JobStatus.IN_PROGRESS
        ).count()
        
        completed = self.db.query(Job).filter(
            Job.job_type == JobType.REBALANCE,
            Job.status == JobStatus.COMPLETED
        ).count()
        
        failed = self.db.query(Job).filter(
            Job.job_type == JobType.REBALANCE,
            Job.status == JobStatus.FAILED
        ).count()
        
        total = pending + in_progress + completed + failed
        is_active = (pending + in_progress) > 0
        
        progress_percent = (completed / total * 100) if total > 0 else 0
        
        return {
            "is_active": is_active,
            "total_jobs": total,
            "pending": pending,
            "in_progress": in_progress,
            "completed": completed,
            "failed": failed,
            "progress_percent": round(progress_percent, 1)
        }
    
    def auto_rebalance_if_needed(self, threshold: float = 80.0, max_migrations: int = 50) -> Dict:
        """Automatically rebalance if cluster balance falls below threshold"""
        analysis = self.analyze_distribution()
        
        if "error" in analysis:
            return {"triggered": False, "reason": analysis["error"]}
        
        if analysis["balance_score"] >= threshold:
            return {
                "triggered": False,
                "reason": f"Cluster balanced (score: {analysis['balance_score']}%)"
            }
        
        # Check if rebalancing is already in progress
        status = self.get_rebalancing_status()
        if status["is_active"]:
            return {
                "triggered": False,
                "reason": "Rebalancing already in progress"
            }
        
        # Create and execute rebalancing plan
        migrations = self.create_rebalancing_plan(max_migrations)
        
        if not migrations:
            return {
                "triggered": False,
                "reason": "No valid migrations found"
            }
        
        jobs_created = self.execute_rebalancing(migrations)
        
        return {
            "triggered": True,
            "balance_score": analysis["balance_score"],
            "migrations_planned": len(migrations),
            "jobs_created": jobs_created
        }


def create_rebalance_job_type():
    """Add REBALANCE to JobType enum if not present"""
    # This should be added to models.py JobType enum:
    # REBALANCE = "rebalance"
    pass