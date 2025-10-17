"""
TossIt Debug Logger
Tracks and logs all decision-making processes for uploads, replication, and storage
"""

import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

class UploadDebugLogger:
    """Logs detailed information about upload decisions"""
    
    def __init__(self, log_dir: str = "./debug_logs"):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self.current_upload = None
        
    def start_upload(self, filename: str, file_size: int, settings: Dict[str, Any]):
        """Start logging a new upload"""
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"upload_{timestamp}_{filename.replace('/', '_')}.json"
        
        self.current_upload = {
            "filename": filename,
            "file_size_bytes": file_size,  # Fixed: use file_size parameter
            "file_size_mb": round(file_size / (1024**2), 2) if file_size > 0 else 0,
            "timestamp": datetime.utcnow().isoformat(),
            "settings": settings,
            "nodes_available": [],
            "chunks": [],
            "decisions": [],
            "summary": {}
        }
        self.log_file = log_file
        
        self.log_decision("upload_started", f"Beginning upload of {filename} ({file_size} bytes)")
    
    def update_file_size(self, total_bytes: int):
        """Update the file size during streaming upload"""
        if self.current_upload:
            self.current_upload["file_size_bytes"] = total_bytes
            self.current_upload["file_size_mb"] = round(total_bytes / (1024**2), 2)
            
            self.log_decision(
                "size_updated", 
                f"File size updated: {total_bytes} bytes ({total_bytes / (1024**2):.2f} MB)"
            )
    
    def log_nodes_snapshot(self, nodes: List[Dict[str, Any]]):
        """Log the state of all nodes at upload start"""
        self.current_upload["nodes_available"] = [
            {
                "name": n["name"],
                "id": n["id"],
                "total_capacity_gb": n["total_capacity_gb"],
                "free_capacity_gb": n["free_capacity_gb"],
                "used_capacity_gb": n["total_capacity_gb"] - n["free_capacity_gb"],
                "utilization_percent": round(((n["total_capacity_gb"] - n["free_capacity_gb"]) / n["total_capacity_gb"]) * 100, 1),
                "priority_score": n["priority_score"],
                "status": n["status"],
                "cpu_score": n["cpu_score"],
                "network_speed_mbps": n.get("network_speed_mbps", 0)
            }
            for n in nodes
        ]
        
        self.log_decision(
            "cluster_state", 
            f"Available nodes: {len(nodes)}, Total capacity: {sum(n['total_capacity_gb'] for n in nodes):.1f} GB"
        )
    
    def log_chunk_decision(self, chunk_index: int, chunk_size: int, primary_node: Dict[str, Any], 
                          all_nodes: List[Dict[str, Any]], reason: str):
        """Log why a specific node was chosen for a chunk"""
        
        # Rank all nodes by priority for comparison
        ranked_nodes = sorted(all_nodes, key=lambda n: n.get("priority_score", 0), reverse=True)
        
        chunk_info = {
            "chunk_index": chunk_index,
            "chunk_size_bytes": chunk_size,
            "chunk_size_mb": round(chunk_size / (1024**2), 2),
            "primary_node": {
                "name": primary_node["name"],
                "id": primary_node["id"],
                "priority_score": primary_node["priority_score"],
                "free_gb_before": primary_node["free_capacity_gb"],
                "free_gb_after": primary_node["free_capacity_gb"] - (chunk_size / (1024**3))
            },
            "decision_reason": reason,
            "node_rankings": [
                {
                    "rank": idx + 1,
                    "name": n["name"],
                    "priority_score": n["priority_score"],
                    "free_gb": n["free_capacity_gb"],
                    "selected": n["id"] == primary_node["id"]
                }
                for idx, n in enumerate(ranked_nodes)
            ]
        }
        
        self.current_upload["chunks"].append(chunk_info)
        
        self.log_decision(
            "chunk_placement",
            f"Chunk {chunk_index} → {primary_node['name']} (Priority: {primary_node['priority_score']:.3f}, Reason: {reason})"
        )
    
    def log_replication_decision(self, chunk_index: int, replica_nodes: List[Dict[str, Any]], 
                                strategy: str, redundancy_mode: str):
        """Log replication decisions for a chunk"""
        
        replication_info = {
            "chunk_index": chunk_index,
            "strategy": strategy,
            "redundancy_mode": redundancy_mode,
            "replica_count": len(replica_nodes),
            "replica_nodes": [
                {
                    "name": n["name"],
                    "id": n["id"],
                    "priority_score": n["priority_score"],
                    "free_gb": n["free_capacity_gb"]
                }
                for n in replica_nodes
            ]
        }
        
        # Add to existing chunk info
        if chunk_index < len(self.current_upload["chunks"]):
            self.current_upload["chunks"][chunk_index]["replication"] = replication_info
        
        node_names = ", ".join(n["name"] for n in replica_nodes)
        self.log_decision(
            "replication_scheduled",
            f"Chunk {chunk_index} replicas → [{node_names}] (Strategy: {strategy}, Mode: {redundancy_mode})"
        )
    
    def log_decision(self, decision_type: str, description: str, metadata: Dict[str, Any] = None):
        """Log a general decision"""
        decision = {
            "timestamp": datetime.utcnow().isoformat(),
            "type": decision_type,
            "description": description
        }
        
        if metadata:
            decision["metadata"] = metadata
        
        self.current_upload["decisions"].append(decision)
        
        # Also print to console for real-time monitoring
        print(f"[DEBUG] {decision_type}: {description}")
    
    def calculate_summary(self):
        """Calculate upload summary statistics"""
        chunks = self.current_upload["chunks"]
        nodes = self.current_upload["nodes_available"]
        
        # Count chunks per node (primary)
        chunks_per_node = {}
        for chunk in chunks:
            node_name = chunk["primary_node"]["name"]
            chunks_per_node[node_name] = chunks_per_node.get(node_name, 0) + 1
        
        # Calculate total replicas needed
        total_replicas_needed = 0
        for chunk in chunks:
            if "replication" in chunk:
                total_replicas_needed += chunk["replication"]["replica_count"]
        
        # Calculate distribution metrics
        total_chunks = len(chunks)
        ideal_per_node = total_chunks / len(nodes) if nodes else 0
        
        distribution_balance = {}
        for node_name, count in chunks_per_node.items():
            deviation = abs(count - ideal_per_node)
            distribution_balance[node_name] = {
                "chunks": count,
                "ideal": round(ideal_per_node, 1),
                "deviation": round(deviation, 1),
                "percentage": round((count / total_chunks) * 100, 1) if total_chunks > 0 else 0
            }
        
        # Calculate actual total size from chunks for accuracy
        actual_total_size_mb = sum(c["chunk_size_mb"] for c in chunks)
        
        self.current_upload["summary"] = {
            "total_chunks": total_chunks,
            "total_size_mb": actual_total_size_mb,
            "chunks_per_node": chunks_per_node,
            "distribution_balance": distribution_balance,
            "total_replicas_scheduled": total_replicas_needed,
            "total_storage_needed_mb": round(
                (actual_total_size_mb * (1 + (total_replicas_needed / total_chunks if total_chunks > 0 else 0))),
                2
            ),
            "most_loaded_node": max(chunks_per_node.items(), key=lambda x: x[1]) if chunks_per_node else None,
            "least_loaded_node": min(chunks_per_node.items(), key=lambda x: x[1]) if chunks_per_node else None,
            "distribution_efficiency": self._calculate_distribution_efficiency(chunks_per_node, nodes)
        }
        
        self.log_decision(
            "upload_summary",
            f"Distribution: {chunks_per_node}, Total replicas: {total_replicas_needed}"
        )
    
    def _calculate_distribution_efficiency(self, chunks_per_node: Dict[str, int], nodes: List[Dict]) -> float:
        """Calculate how evenly distributed the chunks are (0-100%)"""
        if not chunks_per_node or not nodes:
            return 0.0
        
        total_chunks = sum(chunks_per_node.values())
        ideal_per_node = total_chunks / len(nodes)
        
        # Calculate standard deviation
        deviations = [abs(count - ideal_per_node) for count in chunks_per_node.values()]
        avg_deviation = sum(deviations) / len(deviations)
        
        # Convert to efficiency percentage (lower deviation = higher efficiency)
        efficiency = max(0, 100 - (avg_deviation / ideal_per_node * 100)) if ideal_per_node > 0 else 0
        
        return round(efficiency, 1)
    
    def finish_upload(self, success: bool = True, error: str = None, final_size_bytes: int = None):
        """Finalize the upload log"""
        # Update final size if provided
        if final_size_bytes is not None:
            self.update_file_size(final_size_bytes)
        
        self.calculate_summary()
        
        self.current_upload["upload_completed"] = datetime.utcnow().isoformat()
        self.current_upload["success"] = success
        
        if error:
            self.current_upload["error"] = error
        
        # Write to file
        with open(self.log_file, 'w') as f:
            json.dump(self.current_upload, f, indent=2)
        
        print(f"\n[DEBUG] Upload log saved to: {self.log_file}")
        print(f"[DEBUG] Summary: {self.current_upload['summary']['total_chunks']} chunks, "
              f"{self.current_upload['summary']['distribution_efficiency']}% distribution efficiency")
        print(f"[DEBUG] Final size: {self.current_upload['file_size_mb']:.2f} MB")
        
        return self.log_file
    
    def generate_report(self) -> str:
        """Generate a human-readable report"""
        if not self.current_upload:
            return "No upload data available"
        
        report_lines = [
            "=" * 80,
            f"TOSSIT UPLOAD DEBUG REPORT",
            "=" * 80,
            f"File: {self.current_upload['filename']}",
            f"Size: {self.current_upload['file_size_mb']:.2f} MB ({self.current_upload['file_size_bytes']} bytes)",
            f"Timestamp: {self.current_upload['timestamp']}",
            "",
            "SETTINGS:",
            f"  Chunk Size: {self.current_upload['settings'].get('chunk_size_mb', 'N/A')} MB",
            f"  Replication Strategy: {self.current_upload['settings'].get('replication_strategy', 'N/A')}",
            f"  Redundancy Mode: {self.current_upload['settings'].get('redundancy_mode', 'N/A')}",
            f"  Min Replicas: {self.current_upload['settings'].get('min_replicas', 'N/A')}",
            "",
            "CLUSTER STATE:",
        ]
        
        for node in self.current_upload['nodes_available']:
            report_lines.append(
                f"  {node['name']}: {node['free_capacity_gb']:.1f} GB free / {node['total_capacity_gb']:.1f} GB total "
                f"({node['utilization_percent']}% used) - Priority: {node['priority_score']:.3f}"
            )
        
        summary = self.current_upload['summary']
        report_lines.extend([
            "",
            "DISTRIBUTION SUMMARY:",
            f"  Total Chunks: {summary['total_chunks']}",
            f"  Total Size: {summary['total_size_mb']:.2f} MB",
            f"  Total Replicas: {summary['total_replicas_scheduled']}",
            f"  Distribution Efficiency: {summary['distribution_efficiency']}%",
            ""
        ])
        
        for node_name, stats in summary['distribution_balance'].items():
            report_lines.append(
                f"  {node_name}: {stats['chunks']} chunks ({stats['percentage']}%) "
                f"[Ideal: {stats['ideal']}, Deviation: {stats['deviation']}]"
            )
        
        report_lines.extend([
            "",
            f"Most Loaded Node: {summary['most_loaded_node'][0]} ({summary['most_loaded_node'][1]} chunks)" if summary['most_loaded_node'] else "N/A",
            f"Least Loaded Node: {summary['least_loaded_node'][0]} ({summary['least_loaded_node'][1]} chunks)" if summary['least_loaded_node'] else "N/A",
            "",
            "=" * 80
        ])
        
        return "\n".join(report_lines)


# Singleton instance
debug_logger = UploadDebugLogger()