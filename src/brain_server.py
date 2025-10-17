"""
TossIt Brain Server v2 - With actual chunk storage
Run with: python3 brain_server.py
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta, timezone
import hashlib
import aiohttp
import asyncio
import io
from fastapi import Request
from contextlib import asynccontextmanager

from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import sessionmaker
import time
import random
import socket
import urllib.parse



from models import (
    Base, Node, File as FileModel, Chunk, Replica, Job, AuditLog,
    NodeStatus, JobStatus, JobType
)
from tossit_settings import (
    ClusterSettings, get_settings, update_settings, get_replica_nodes
)
from tossit_debug_logger import debug_logger
from tossit_rebalancer import ClusterRebalancer

app = FastAPI(title="TossIt Brain")

# Create engine with better SQLite configuration
engine = create_engine(
    'sqlite:///tossit_brain.db', 
    echo=False,
    poolclass=StaticPool,
    connect_args={
        "check_same_thread": False,
        "timeout": 20,  # 20 second timeout
        "isolation_level": None  # Enable autocommit mode
    },
    pool_pre_ping=True,  # Verify connections before use
    pool_recycle=3600   # Recycle connections every hour
)

Base.metadata.create_all(engine)
ClusterSettings.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)



CHUNK_SIZE = 64 * 1024 * 1024  # 64 MB (will be overridden by settings)
MIN_REPLICAS = 2  # Will be overridden by settings

def get_local_ip():
    """Get the local IP address of this machine"""
    try:
        # Connect to a remote address to determine our local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception:
        # Fallback to localhost if we can't determine IP
        return "127.0.0.1"

# Add retry decorator for database operations
def retry_on_database_lock(max_retries=3, base_delay=0.1):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                        # Exponential backoff with jitter
                        delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                        print(f"Database locked, retrying in {delay:.2f}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        raise e
            return None
        return wrapper
    return decorator


def get_chunk_size(db):
    """Get chunk size from settings"""
    settings = get_settings(db)
    return settings.chunk_size_mb * 1024 * 1024


def calculate_priority_score(node: Node) -> float:
    """Calculate priority score based on node characteristics with improved weighting"""
    
    # 1. Capacity score: More available space = higher priority
    if node.total_capacity_gb > 0:
        capacity_ratio = node.free_capacity_gb / node.total_capacity_gb
        # Give bonus for nodes with lots of absolute free space
        absolute_bonus = min(1.0, node.free_capacity_gb / 20.0)  # Bonus up to 20GB
        capacity_score = (capacity_ratio * 0.7) + (absolute_bonus * 0.3)
    else:
        capacity_score = 0.0
    
    # 2. CPU score: Normalize based on reasonable expectations
    # Assume 1-16 CPU cores typical, normalize to 0-1 range
    cpu_normalized = min(1.0, node.cpu_score / 16.0)
    
    # 3. Current load penalty: Brain nodes should have lower priority
    load_penalty = 0.0
    if node.is_brain:
        load_penalty = 0.2  # 20% penalty for brain nodes
    
    # 4. Network speed bonus (future-proofing)
    network_bonus = min(0.1, node.network_speed_mbps / 10000.0)  # Small bonus for fast networks
    
    # 5. Uptime reliability
    uptime_score = node.avg_uptime_percent / 100.0
    
    # Calculate final score with improved weighting
    base_score = (
        capacity_score * 0.60 +    # Capacity is most important
        cpu_normalized * 0.20 +    # CPU matters for processing
        uptime_score * 0.15 +      # Reliability matters
        network_bonus * 0.05       # Network speed minor factor
    )
    
    # Apply penalties
    final_score = base_score - load_penalty
    
    # Ensure score stays in reasonable range
    final_score = max(0.0, min(1.0, final_score))
    
    return final_score


def select_primary_node(db):
    """Select the best node to store primary replica with better logic"""
    nodes = db.query(Node).filter(
        Node.status == NodeStatus.ONLINE,
        Node.free_capacity_gb > 1.0  # Must have at least 1GB free
    ).all()
    
    if not nodes:
        return None
    
    # Update priority scores for all nodes
    for node in nodes:
        node.priority_score = calculate_priority_score(node)
    
    # Get current chunk distribution to balance load
    chunk_counts = {}
    for node in nodes:
        chunk_counts[node.id] = db.query(Chunk).filter(
            Chunk.primary_node_id == node.id
        ).count()
    
    # Calculate load-balanced priority
    total_chunks = sum(chunk_counts.values())
    ideal_per_node = total_chunks / len(nodes) if len(nodes) > 0 else 0
    
    best_node = None
    best_adjusted_score = -1
    
    for node in nodes:
        current_chunks = chunk_counts.get(node.id, 0)
        
        # Penalty for nodes with too many chunks
        if ideal_per_node > 0:
            load_deviation = (current_chunks - ideal_per_node) / ideal_per_node
            load_penalty = max(0, load_deviation * 0.3)  # Up to 30% penalty for overloaded nodes
        else:
            load_penalty = 0
        
        # Bonus for nodes with very few chunks
        if current_chunks < ideal_per_node * 0.5:
            underload_bonus = 0.2
        else:
            underload_bonus = 0
        
        adjusted_score = node.priority_score - load_penalty + underload_bonus
        
        print(f"Node {node.name}: base_priority={node.priority_score:.3f}, chunks={current_chunks}, "
              f"load_penalty={load_penalty:.3f}, bonus={underload_bonus:.3f}, "
              f"final_score={adjusted_score:.3f}")
        
        if adjusted_score > best_adjusted_score:
            best_adjusted_score = adjusted_score
            best_node = node
    
    # Commit the updated priority scores
    db.commit()
    
    return best_node


def get_node_load_info(db):
    """Get detailed information about node loads for debugging"""
    nodes = db.query(Node).filter(Node.status == NodeStatus.ONLINE).all()
    
    load_info = []
    for node in nodes:
        chunk_count = db.query(Chunk).filter(Chunk.primary_node_id == node.id).count()
        replica_count = db.query(Replica).filter(Replica.node_id == node.id).count()
        
        # Calculate usage percentage
        if node.total_capacity_gb > 0:
            usage_percent = ((node.total_capacity_gb - node.free_capacity_gb) / node.total_capacity_gb) * 100
        else:
            usage_percent = 0
        
        load_info.append({
            "name": node.name,
            "priority_score": node.priority_score,
            "free_gb": node.free_capacity_gb,
            "total_gb": node.total_capacity_gb,
            "usage_percent": usage_percent,
            "primary_chunks": chunk_count,
            "total_replicas": replica_count,
            "is_brain": node.is_brain,
            "cpu_score": node.cpu_score
        })
    
    return load_info

# In brain_server.py, add node timeout checking:
def check_node_timeouts(db):
    """Mark nodes as offline if they haven't sent heartbeat recently"""
    timeout_threshold = datetime.now(timezone.utc) - timedelta(seconds=300)  # 30 second timeout
        
    offline_nodes = db.query(Node).filter(
        Node.last_heartbeat < timeout_threshold,
        Node.status == NodeStatus.ONLINE
    ).all()
        
    for node in offline_nodes:
        node.status = NodeStatus.OFFLINE
        print(f"⚠ Node {node.name} marked offline (last heartbeat: {node.last_heartbeat})")
        
    if offline_nodes:
        db.commit()

def encode_filename_for_header(filename):
    """Encode filename for HTTP header using RFC 5987"""
    # Try ASCII first (most compatible)
    try:
        filename.encode('ascii')
        return f'attachment; filename="{filename}"'
    except UnicodeEncodeError:
        # Use RFC 5987 encoding for non-ASCII filenames
        encoded_filename = urllib.parse.quote(filename.encode('utf-8'))
        return f"attachment; filename*=UTF-8''{encoded_filename}"


async def send_chunk_to_node(node: Node, chunk_id: int, chunk_data: bytes, is_primary: bool = False):
    """Send chunk data to a node for storage"""
    url = f"http://{node.ip_address}:{node.port}/api/chunks/store"
    
    try:
        async with aiohttp.ClientSession() as session:
            data = aiohttp.FormData()
            data.add_field('chunk_id', str(chunk_id))
            data.add_field('is_primary', 'true' if is_primary else 'false')
            data.add_field('chunk_data', chunk_data, filename=f'chunk_{chunk_id}.dat')
            
            async with session.post(url, data=data, timeout=aiohttp.ClientTimeout(total=300)) as response:
                if response.status == 200:
                    result = await response.json()
                    return result.get('local_path')
                else:
                    print(f"Failed to send chunk {chunk_id} to {node.name}: {response.status}")
                    return None
    except Exception as e:
        print(f"Error sending chunk to {node.name}: {e}")
        return None
    


@app.get("/api/debug/node-loads")
async def debug_node_loads():
    """Debug endpoint to show detailed node load information"""
    db = SessionLocal()
    
    try:
        load_info = get_node_load_info(db)
        
        # Calculate distribution efficiency
        total_chunks = sum(info["primary_chunks"] for info in load_info)
        ideal_per_node = total_chunks / len(load_info) if load_info else 0
        
        # Calculate how far each node is from ideal
        for info in load_info:
            info["deviation_from_ideal"] = info["primary_chunks"] - ideal_per_node
            info["deviation_percent"] = (abs(info["deviation_from_ideal"]) / ideal_per_node * 100) if ideal_per_node > 0 else 0
        
        return {
            "total_chunks": total_chunks,
            "ideal_per_node": round(ideal_per_node, 1),
            "nodes": load_info,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    finally:
        db.close()


async def schedule_replication_jobs(db, chunk: Chunk, primary_node: Node):
    """Schedule jobs to replicate chunk to other nodes"""
    settings = get_settings(db)
    
    # Get replica nodes based on current settings
    replica_nodes = get_replica_nodes(db, primary_node, settings)
    
    for target_node in replica_nodes:
        job = Job(
            job_type=JobType.REPLICATION,
            chunk_id=chunk.id,
            source_node_id=primary_node.id,
            target_node_id=target_node.id,
            priority=7
        )
        db.add(job)
    
    db.commit()


# ===== NODE MANAGEMENT =====

@app.post("/api/nodes/register")
async def register_node(node_data: dict):
    """Register a new node with the Brain"""
    db = SessionLocal()
    
    try:
        existing = db.query(Node).filter_by(name=node_data["name"]).first()
        
        # Determine if this is a brain node (runs on same IP as brain server)
        brain_server_ip = "192.168.74.130"  # Your brain server IP
        is_brain_node = node_data.get("ip_address") == brain_server_ip
        
        if existing:
            for key, value in node_data.items():
                setattr(existing, key, value)
            existing.status = NodeStatus.ONLINE
            existing.last_heartbeat = datetime.utcnow()
            existing.is_brain = is_brain_node
            node = existing
        else:
            node_data["is_brain"] = is_brain_node
            node = Node(**node_data)
            node.priority_score = calculate_priority_score(node)
            db.add(node)
        
        db.commit()
        db.refresh(node)
        
        brain_indicator = "🧠 " if is_brain_node else ""
        log = AuditLog(
            event_type="node_register",
            node_id=node.id,
            details=f"Node {brain_indicator}{node.name} registered"
        )
        db.add(log)
        db.commit()
        
        print(f"✓ Node registered: {brain_indicator}{node.name} (ID: {node.id})")
        
        return {"node_id": node.id, "status": "registered", "is_brain": is_brain_node}
    
    finally:
        db.close()

@app.get("/api/nodes")
async def list_nodes():
    """Get all nodes in the cluster"""
    db = SessionLocal()
    
    try:
        nodes = db.query(Node).all()
        
        return [{
            "id": n.id,
            "name": n.name,
            "status": n.status.value,
            "total_capacity_gb": round(n.total_capacity_gb, 2),
            "free_capacity_gb": round(n.free_capacity_gb, 2),
            "priority_score": round(n.priority_score, 3),
            "is_brain": n.is_brain,
            "last_heartbeat": n.last_heartbeat.isoformat() if n.last_heartbeat else None
        } for n in nodes]
    
    finally:
        db.close()


@app.post("/api/nodes/heartbeat")
async def node_heartbeat(request: Request):
    """Receive heartbeat from a node"""
    try:
        heartbeat_data = await request.json()
    except Exception as e:
        print(f"Failed to parse heartbeat JSON: {e}")
        raise HTTPException(status_code=422, detail="Invalid JSON")
    
    db = SessionLocal()
    
    try:
        node_id = heartbeat_data.get("node_id")
        if not node_id:
            raise HTTPException(status_code=422, detail="node_id required")
            
        node = db.query(Node).filter_by(id=node_id).first()
        
        if not node:
            raise HTTPException(status_code=404, detail="Node not found")
        
        node.free_capacity_gb = heartbeat_data.get("free_capacity_gb", node.free_capacity_gb)
        node.status = NodeStatus.ONLINE
        node.last_heartbeat = datetime.utcnow()
        node.priority_score = calculate_priority_score(node)
        
        db.commit()
        
        return {"status": "ok"}
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Heartbeat processing error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    finally:
        db.close()


@app.get("/api/nodes")
async def list_nodes():
    """Get all nodes in the cluster"""
    db = SessionLocal()
    
    try:
        nodes = db.query(Node).all()
        
        return [{
            "id": n.id,
            "name": n.name,
            "status": n.status.value,
            "total_capacity_gb": round(n.total_capacity_gb, 2),
            "free_capacity_gb": round(n.free_capacity_gb, 2),
            "priority_score": round(n.priority_score, 3),
            "last_heartbeat": n.last_heartbeat.isoformat() if n.last_heartbeat else None
        } for n in nodes]
    
    finally:
        db.close()


# ===== FILE MANAGEMENT =====

@app.get("/api/files")
async def list_files():
    """List all files in the cluster"""
    db = SessionLocal()
    
    try:
        files = db.query(FileModel).all()
        
        return [{
            "id": f.id,
            "filename": f.filename,
            "size_bytes": f.total_size_bytes,
            "size_mb": round(f.total_size_bytes / (1024**2), 2),
            "chunks": f.total_chunks,
            "complete": f.is_complete,
            "replicated": f.is_replicated,
            "uploaded_at": f.created_at.isoformat()
        } for f in files]
    
    finally:
        db.close()


@app.post("/api/files/upload/stream")
async def upload_file_streaming(file: UploadFile = File(...), uploaded_by: str = "anonymous"):
    """Upload a large file using streaming (doesn't load entire file into memory)"""
    db = SessionLocal()
    
    try:
        # Get settings
        settings = get_settings(db)
        chunk_size = get_chunk_size(db)
        
        # Start debug logging
        debug_logger.start_upload(
            file.filename,
            0,  # Size unknown initially for streaming
            {
                "chunk_size_mb": settings.chunk_size_mb,
                "replication_strategy": settings.replication_strategy,
                "redundancy_mode": settings.redundancy_mode,
                "min_replicas": settings.min_replicas,
                "max_replicas": settings.max_replicas
            }
        )
        
        # Log cluster state
        nodes = db.query(Node).filter(Node.status == NodeStatus.ONLINE).all()
        debug_logger.log_nodes_snapshot([{
            "name": n.name,
            "id": n.id,
            "total_capacity_gb": n.total_capacity_gb,
            "free_capacity_gb": n.free_capacity_gb,
            "priority_score": n.priority_score,
            "status": n.status.value,
            "cpu_score": n.cpu_score,
            "network_speed_mbps": n.network_speed_mbps
        } for n in nodes])
        
        # Create file record without knowing total chunks yet
        file_record = FileModel(
            filename=file.filename,
            total_size_bytes=0,  # Will update at the end
            chunk_size_bytes=chunk_size,
            total_chunks=0,  # Will update as we go
            checksum_sha256="",  # Will calculate at the end
            uploaded_by=uploaded_by,
            mime_type=file.content_type,
            is_complete=False
        )
        db.add(file_record)
        db.commit()
        db.refresh(file_record)
        
        print(f"Streaming upload: {file.filename}")
        
        chunk_index = 0
        total_bytes = 0
        file_hasher = hashlib.sha256()
        
        # Stream file in chunks - use file.file directly for true streaming
        buffer = bytearray()
        read_size = 65536  # Read 64KB at a time from upload stream
        
        while True:
            # Read data from upload stream - this is the KEY: read from file.file, not file.read()
            data = file.file.read(read_size)
            if not data:
                break
            
            buffer.extend(data)
            file_hasher.update(data)
            total_bytes += len(data)
            
            # When buffer reaches chunk size, process it
            while len(buffer) >= chunk_size:
                chunk_data = bytes(buffer[:chunk_size])
                buffer = buffer[chunk_size:]
                
                # Process this chunk
                await process_and_store_chunk(
                    db, file_record.id, chunk_index, 
                    chunk_data, chunk_size
                )
                
                chunk_index += 1
                
                # Clear chunk_data to free memory immediately
                del chunk_data
                
                if chunk_index % 10 == 0:
                    print(f"  Processed {chunk_index} chunks ({total_bytes / (1024**3):.2f} GB)")
        
        # Process remaining data in buffer as final chunk
        if len(buffer) > 0:
            chunk_data = bytes(buffer)
            await process_and_store_chunk(
                db, file_record.id, chunk_index, 
                chunk_data, len(chunk_data)
            )
            chunk_index += 1
            del chunk_data
        
        # Clear buffer
        del buffer
        
        # Update file record with final info
        file_record.total_size_bytes = total_bytes
        file_record.total_chunks = chunk_index
        file_record.checksum_sha256 = file_hasher.hexdigest()
        file_record.is_complete = True
        db.commit()
        
        print(f"✓ Streaming upload complete: {file.filename} ({total_bytes / (1024**3):.2f} GB, {chunk_index} chunks)")
        
        # Finish debug logging
        debug_logger.update_file_size(total_bytes)
        debug_logger.finish_upload(success=True)
        print("\n" + debug_logger.generate_report())
        
        log = AuditLog(
            event_type="file_upload",
            file_id=file_record.id,
            details=f"Uploaded {file.filename} (streaming)",
            user=uploaded_by,
            success=True
        )
        db.add(log)
        db.commit()
        
        return {
            "file_id": file_record.id,
            "filename": file.filename,
            "total_chunks": chunk_index,
            "total_size_bytes": total_bytes,
            "chunk_size_bytes": chunk_size
        }
    
    except Exception as e:
        print(f"✗ Streaming upload failed: {e}")
        import traceback
        traceback.print_exc()
        debug_logger.finish_upload(success=False, error=str(e))
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        db.close()


async def process_and_store_chunk(db, file_id: int, chunk_index: int, chunk_data: bytes, chunk_size: int):
    """Process and store a single chunk"""
    chunk_checksum = hashlib.sha256(chunk_data).hexdigest()
    
    # Get all nodes for logging
    all_nodes = db.query(Node).filter(Node.status == NodeStatus.ONLINE).all()
    all_nodes_info = [{
        "name": n.name,
        "id": n.id,
        "total_capacity_gb": n.total_capacity_gb,
        "free_capacity_gb": n.free_capacity_gb,
        "priority_score": n.priority_score,
        "cpu_score": n.cpu_score,
        "network_speed_mbps": n.network_speed_mbps
    } for n in all_nodes]
    
    # Select primary node
    primary_node = select_primary_node(db)
    
    if not primary_node:
        raise Exception("No available nodes")
    
    # Log chunk placement decision
    primary_node_info = next((n for n in all_nodes_info if n["id"] == primary_node.id), None)
    reason = f"Highest priority score ({primary_node.priority_score:.3f}), {primary_node.free_capacity_gb:.1f} GB free"
    
    debug_logger.log_chunk_decision(
        chunk_index,
        chunk_size,
        primary_node_info,
        all_nodes_info,
        reason
    )
    
    # Create chunk record
    chunk = Chunk(
        file_id=file_id,
        chunk_index=chunk_index,
        size_bytes=chunk_size,
        checksum_sha256=chunk_checksum,
        primary_node_id=primary_node.id
    )
    db.add(chunk)
    db.commit()
    db.refresh(chunk)
    
    # Send chunk data to primary node
    local_path = await send_chunk_to_node(primary_node, chunk.id, chunk_data, is_primary=True)
    
    if local_path:
        replica = Replica(
            chunk_id=chunk.id,
            node_id=primary_node.id,
            is_primary=True,
            local_path=local_path,
            verification_status="valid"
        )
        db.add(replica)
        db.commit()
        
        # Schedule replication jobs and log decisions
        settings = get_settings(db)
        replica_nodes = get_replica_nodes(db, primary_node, settings)
        
        replica_nodes_info = [{
            "name": n.name,
            "id": n.id,
            "priority_score": n.priority_score,
            "free_capacity_gb": n.free_capacity_gb
        } for n in replica_nodes]
        
        debug_logger.log_replication_decision(
            chunk_index,
            replica_nodes_info,
            settings.replication_strategy,
            settings.redundancy_mode
        )
        
        await schedule_replication_jobs(db, chunk, primary_node)


@app.post("/api/files/upload")
async def upload_file(file: UploadFile = File(...), uploaded_by: str = "anonymous"):
    """Upload a file to the cluster"""
    db = SessionLocal()
    
    try:
        content = await file.read()
        total_size = len(content)
        
        # Get chunk size from settings
        chunk_size = get_chunk_size(db)
        
        file_checksum = hashlib.sha256(content).hexdigest()
        
        file_record = FileModel(
            filename=file.filename,
            total_size_bytes=total_size,
            chunk_size_bytes=chunk_size,
            total_chunks=(total_size + chunk_size - 1) // chunk_size,
            checksum_sha256=file_checksum,
            uploaded_by=uploaded_by,
            mime_type=file.content_type
        )
        db.add(file_record)
        db.commit()
        db.refresh(file_record)
        
        print(f"Uploading file: {file.filename} ({total_size} bytes)")
        
        chunks_created = []
        for i in range(0, total_size, chunk_size):
            chunk_data = content[i:i + chunk_size]
            chunk_index = i // chunk_size
            chunk_checksum = hashlib.sha256(chunk_data).hexdigest()
            
            primary_node = select_primary_node(db)
            
            if not primary_node:
                raise HTTPException(status_code=503, detail="No available nodes")
            
            chunk = Chunk(
                file_id=file_record.id,
                chunk_index=chunk_index,
                size_bytes=len(chunk_data),
                checksum_sha256=chunk_checksum,
                primary_node_id=primary_node.id
            )
            db.add(chunk)
            db.commit()
            db.refresh(chunk)
            
            # Send chunk data to primary node
            local_path = await send_chunk_to_node(primary_node, chunk.id, chunk_data, is_primary=True)
            
            if local_path:
                replica = Replica(
                    chunk_id=chunk.id,
                    node_id=primary_node.id,
                    is_primary=True,
                    local_path=local_path,
                    verification_status="valid"
                )
                db.add(replica)
                db.commit()
                
                print(f"  Chunk {chunk_index} → Node {primary_node.name}")
                
                # Schedule replication jobs
                await schedule_replication_jobs(db, chunk, primary_node)
            else:
                print(f"  Failed to store chunk {chunk_index}")
            
            chunks_created.append(chunk.id)
        
        file_record.is_complete = True
        db.commit()
        
        log = AuditLog(
            event_type="file_upload",
            file_id=file_record.id,
            details=f"Uploaded {file.filename}",
            user=uploaded_by,
            success=True
        )
        db.add(log)
        db.commit()
        
        print(f"✓ Upload complete: {len(chunks_created)} chunks")
        
        return {
            "file_id": file_record.id,
            "filename": file.filename,
            "total_chunks": len(chunks_created),
            "chunks": chunks_created,
            "total_size_bytes": total_size,
            "chunk_size_bytes": chunk_size
        }
    
    finally:
        db.close()


@app.get("/api/files/{file_id}/info")
async def file_info(file_id: int):
    """Get detailed info about a file and its chunks"""
    db = SessionLocal()
    
    try:
        file_record = db.query(FileModel).filter_by(id=file_id).first()
        
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")
        
        chunks = db.query(Chunk).filter_by(file_id=file_id).order_by(Chunk.chunk_index).all()
        
        chunk_info = []
        for chunk in chunks:
            replicas = db.query(Replica).filter_by(chunk_id=chunk.id).all()
            replica_details = []
            
            for replica in replicas:
                node = db.query(Node).filter_by(id=replica.node_id).first()
                replica_details.append({
                    "node_name": node.name if node else "unknown",
                    "node_id": replica.node_id,
                    "is_primary": replica.is_primary,
                    "local_path": replica.local_path
                })
            
            chunk_info.append({
                "chunk_id": chunk.id,
                "chunk_index": chunk.chunk_index,
                "size_bytes": chunk.size_bytes,
                "replicas": replica_details
            })
        
        return {
            "file_id": file_record.id,
            "filename": file_record.filename,
            "total_size": file_record.total_size_bytes,
            "is_complete": file_record.is_complete,
            "chunks": chunk_info
        }
    
    finally:
        db.close()


@app.get("/api/files/{file_id}/download")
async def download_file(file_id: int):
    """Download a file from the cluster"""
    db = SessionLocal()
    
    file_record = db.query(FileModel).filter_by(id=file_id).first()
    
    if not file_record:
        db.close()
        raise HTTPException(status_code=404, detail="File not found")
    
    if not file_record.is_complete:
        db.close()
        raise HTTPException(status_code=400, detail="File upload not complete")
    
    # Get all chunks in order
    chunks = db.query(Chunk).filter_by(file_id=file_id).order_by(Chunk.chunk_index).all()
    
    # Get chunk locations BEFORE closing the database
    chunk_locations = []
    for chunk in chunks:
        replica = db.query(Replica).filter_by(
            chunk_id=chunk.id,
            is_primary=True
        ).first()
        
        if replica:
            node = db.query(Node).filter_by(id=replica.node_id).first()
            if node:
                chunk_locations.append({
                    'chunk_id': chunk.id,
                    'chunk_index': chunk.chunk_index,
                    'size_bytes': chunk.size_bytes,
                    'url': f"http://{node.ip_address}:{node.port}/api/chunks/{chunk.id}/data",
                    'node_name': node.name
                })
    
    # Update last accessed
    file_record.last_accessed = datetime.utcnow()
    
    # Log download
    log = AuditLog(
        event_type="file_download",
        file_id=file_id,
        details=f"Downloaded {file_record.filename}"
    )
    db.add(log)
    db.commit()
    
    # Store values we need
    filename = file_record.filename
    mime_type = file_record.mime_type
    
    # NOW we can close the database
    db.close()
    
    print(f"Starting download of {filename} ({len(chunk_locations)} chunks)")
    
    async def file_stream():
        for location in chunk_locations:
            print(f"Fetching chunk {location['chunk_index']} from {location['node_name']}")
            
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(location['url'], timeout=aiohttp.ClientTimeout(total=60)) as response:
                        if response.status == 200:
                            async for data in response.content.iter_chunked(8192):
                                yield data
                            print(f"✓ Chunk {location['chunk_index']} downloaded ({location['size_bytes']} bytes)")
                        else:
                            print(f"✗ Failed to fetch chunk {location['chunk_index']}: HTTP {response.status}")
            except Exception as e:
                print(f"✗ Error fetching chunk {location['chunk_index']}: {e}")
    
    content_disposition = encode_filename_for_header(filename)
    return StreamingResponse(
        file_stream(),
        media_type=mime_type or "application/octet-stream",
        headers={
            "Content-Disposition": content_disposition #f'attachment; filename="{}"'
        }
    )


# ===== JOB MANAGEMENT =====

@app.get("/api/jobs/next")
async def get_next_job(node_id: int):
    """Get the next pending job for a node"""
    db = SessionLocal()
    
    try:
        job = db.query(Job).filter(
            Job.status == JobStatus.PENDING,
            Job.target_node_id == node_id
        ).order_by(desc(Job.priority)).first()
        
        if not job:
            return None
        
        job.status = JobStatus.IN_PROGRESS
        job.started_at = datetime.utcnow()
        db.commit()
        
        source_node = db.query(Node).filter_by(id=job.source_node_id).first()
        chunk = db.query(Chunk).filter_by(id=job.chunk_id).first()
        
        return {
            "id": job.id,
            "job_type": job.job_type.value,
            "chunk_id": job.chunk_id,
            "expected_checksum": chunk.checksum_sha256 if chunk else None,
            "source_node_url": f"http://{source_node.ip_address}:{source_node.port}" if source_node else None
        }
    
    finally:
        db.close()


@app.post("/api/jobs/update")
async def update_job(update_data: dict):
    """Update job status"""
    db = SessionLocal()
    
    try:
        job = db.query(Job).filter_by(id=update_data["job_id"]).first()
        
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        job.status = JobStatus[update_data["status"].upper()]
        job.completed_at = datetime.utcnow()
        
        if update_data.get("error_message"):
            job.error_message = update_data["error_message"]
        
        db.commit()
        
        return {"status": "updated"}
    
    finally:
        db.close()


@app.post("/api/replicas/create")
async def create_replica(replica_data: dict):
    """Record that a replica has been created"""
    db = SessionLocal()
    
    try:
        replica = Replica(
            chunk_id=replica_data["chunk_id"],
            node_id=replica_data["node_id"],
            local_path=replica_data["local_path"],
            verification_status="valid"
        )
        db.add(replica)
        db.commit()
        
        print(f"✓ Replica created: Chunk {replica_data['chunk_id']} → Node {replica_data['node_id']}")
        
        return {"status": "created"}
    
    finally:
        db.close()


@app.delete("/api/files/{file_id}")
async def delete_file(file_id: int):
    """Delete a file and all its chunks from the cluster"""
    db = SessionLocal()
    
    try:
        file_record = db.query(FileModel).filter_by(id=file_id).first()
        
        if not file_record:
            raise HTTPException(status_code=404, detail="File not found")
        
        filename = file_record.filename
        
        # Get all chunks for this file
        chunks = db.query(Chunk).filter_by(file_id=file_id).all()
        
        print(f"Deleting file: {filename} ({len(chunks)} chunks)")
        
        # Delete chunks from nodes first
        for chunk in chunks:
            replicas = db.query(Replica).filter_by(chunk_id=chunk.id).all()
            
            for replica in replicas:
                node = db.query(Node).filter_by(id=replica.node_id).first()
                if node:
                    try:
                        async with aiohttp.ClientSession() as session:
                            url = f"http://{node.ip_address}:{node.port}/api/chunks/{chunk.id}/delete"
                            async with session.delete(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                                if response.status == 200:
                                    print(f"  Deleted chunk {chunk.id} from {node.name}")
                                else:
                                    print(f"  Failed to delete chunk {chunk.id} from {node.name}")
                    except Exception as e:
                        print(f"  Error deleting chunk {chunk.id} from {node.name}: {e}")
                
                # Delete replica record
                db.delete(replica)
            
            # Delete chunk record
            db.delete(chunk)
        
        # Delete file record
        db.delete(file_record)
        
        # Log deletion
        log = AuditLog(
            event_type="file_delete",
            file_id=file_id,
            details=f"Deleted {filename}",
            success=True
        )
        db.add(log)
        
        db.commit()
        
        print(f"File deleted: {filename}")
        
        return {"status": "deleted", "filename": filename}
    
    except Exception as e:
        db.rollback()
        print(f"Error deleting file: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        db.close()


@app.get("/api/settings")
async def get_cluster_settings():
    """Get current cluster settings"""
    db = SessionLocal()
    
    try:
        settings = get_settings(db)
        
        return {
            "chunk_size_mb": settings.chunk_size_mb,
            "min_replicas": settings.min_replicas,
            "max_replicas": settings.max_replicas,
            "replication_strategy": settings.replication_strategy,
            "redundancy_mode": settings.redundancy_mode,
            "enable_striping": settings.enable_striping,
            "stripe_width": settings.stripe_width,
            "verify_on_upload": settings.verify_on_upload,
            "parallel_uploads": settings.parallel_uploads,
            "parallel_downloads": settings.parallel_downloads
        }
    
    finally:
        db.close()


@app.post("/api/settings")
async def update_cluster_settings(settings_data: dict):
    """Update cluster settings"""
    db = SessionLocal()
    
    try:
        settings = update_settings(db, settings_data)
        
        print(f"✓ Settings updated: {settings_data}")
        
        return {"status": "updated", "settings": settings_data}
    
    finally:
        db.close()


@app.get("/settings", response_class=HTMLResponse)
async def settings_page():
    """Settings page"""
    db = SessionLocal()
    
    try:
        settings = get_settings(db)
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>TossIt - Cluster Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                h1 {{ color: #333; }}
                .stats {{ display: flex; gap: 20px; margin: 20px 0; }}
                .stat-box {{ background: white; padding: 20px; border-radius: 8px; flex: 1; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .stat-box h3 {{ margin: 0 0 10px 0; color: #666; font-size: 14px; }}
                .stat-box .value {{ font-size: 32px; font-weight: bold; color: #2196F3; }}
                table {{ width: 100%; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-collapse: collapse; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
                th {{ background: #2196F3; color: white; font-weight: bold; }}
                tr:last-child td {{ border-bottom: none; }}
                .status-online {{ color: #4CAF50; font-weight: bold; }}
                .status-offline {{ color: #f44336; font-weight: bold; }}
                .status-degraded {{ color: #FF9800; font-weight: bold; }}
                .section {{ margin: 30px 0; }}
                .upload-section {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 30px; }}
                .upload-form {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
                .file-input {{ padding: 10px; border: 2px solid #ddd; border-radius: 4px; flex: 1; min-width: 250px; }}
                .upload-btn {{ background: #2196F3; color: white; padding: 12px 24px; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; }}
                .upload-btn:hover {{ background: #1976D2; }}
                .upload-btn:disabled {{ background: #ccc; cursor: not-allowed; }}
                .brain-node {{ font-weight: bold; color: #FF6B6B; }}
                .node-name {{ display: flex; align-items: center; gap: 5px; }}
                .refresh-indicator {{ 
                    position: fixed; 
                    top: 20px; 
                    right: 20px; 
                    background: #2196F3; 
                    color: white; 
                    padding: 8px 12px; 
                    border-radius: 4px; 
                    font-size: 12px;
                    opacity: 0;
                    transition: opacity 0.3s;
                }}
                .refresh-indicator.visible {{ opacity: 1; }}
                .download-check {{ 
                    background: #fff3cd; 
                    border: 1px solid #ffeaa7; 
                    padding: 15px; 
                    border-radius: 8px; 
                    margin: 10px 0; 
                    display: none; 
                }}
                .node-check {{ margin: 5px 0; padding: 5px; border-radius: 4px; }}
                .node-check.online {{ background: #d4edda; color: #155724; }}
                .node-check.offline {{ background: #f8d7da; color: #721c24; }}
            </style>
        </head>
        <body>
            <div class="refresh-indicator" id="refreshIndicator">
                Refreshing data...
            </div>

            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h1>🗄️ TossIt Cluster Dashboard</h1>
                <div>
                    <a href="/settings" style="background: #666; color: white; padding: 10px 20px; border-radius: 4px; text-decoration: none; font-weight: bold; margin-right: 10px;">⚙️ Settings</a>
                    <button onclick="rebalanceCluster()" id="rebalanceBtn" style="background: #FF9800; color: white; padding: 10px 20px; border-radius: 4px; border: none; cursor: pointer; font-weight: bold;">⚖️ Rebalance Cluster</button>
                </div>
            </div>
            
            <div id="rebalanceStatus" style="display: none; background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #FF9800;">
                <strong>Rebalancing in Progress...</strong>
                <div style="margin-top: 10px;">
                    <div style="background: #f0f0f0; height: 20px; border-radius: 10px; overflow: hidden;">
                        <div id="rebalanceProgress" style="background: #4CAF50; height: 100%; width: 0%; transition: width 0.3s;"></div>
                    </div>
                    <p id="rebalanceText" style="margin-top: 5px; font-size: 14px;"></p>
                </div>
            </div>
            
            <div class="stats" id="statsSection">
                <!-- Stats will be populated by JavaScript -->
            </div>
            
            <div class="upload-section">
                <h2>Upload File</h2>
                <form id="uploadForm" class="upload-form" onsubmit="uploadFile(event)">
                    <input type="file" id="fileInput" class="file-input" required>
                    <button type="submit" class="upload-btn" id="uploadBtn">📤 Upload to Cluster</button>
                </form>
            </div>
            
            <div class="section">
                <h2>Cluster Nodes</h2>
                <table id="nodesTable">
                    <tr>
                        <th>Name</th>
                        <th>Status</th>
                        <th>Capacity</th>
                        <th>Priority Score</th>
                    </tr>
                    <!-- Nodes will be populated by JavaScript -->
                </table>
            </div>
            
            <div class="section">
                <h2>Stored Files</h2>
                <table id="filesTable">
                    <tr>
                        <th>Filename</th>
                        <th>Size</th>
                        <th>Chunks</th>
                        <th>Status</th>
                        <th>Actions</th>
                    </tr>
                    <!-- Files will be populated by JavaScript -->
                </table>
            </div>
            
            <script>
                let lastRefresh = Date.now();
                const REFRESH_INTERVAL = 15000; // Refresh every 15 seconds
                
                // Auto-refresh functionality
                function showRefreshIndicator() {{
                    const indicator = document.getElementById('refreshIndicator');
                    indicator.classList.add('visible');
                    setTimeout(() => indicator.classList.remove('visible'), 2000);
                }}
                
                function updateStats(data) {{
                    const statsHtml = `
                        <div class="stat-box">
                            <h3>Total Capacity</h3>
                            <div class="value">${{data.total_capacity.toFixed(1)}} GB</div>
                        </div>
                        <div class="stat-box">
                            <h3>Used Space</h3>
                            <div class="value">${{data.used_capacity.toFixed(1)}} GB</div>
                        </div>
                        <div class="stat-box">
                            <h3>Active Nodes</h3>
                            <div class="value">${{data.active_nodes}}</div>
                        </div>
                        <div class="stat-box">
                            <h3>Total Files</h3>
                            <div class="value">${{data.total_files}}</div>
                        </div>
                    `;
                    document.getElementById('statsSection').innerHTML = statsHtml;
                }}
                
                function updateNodesTable(nodes) {{
                    const tbody = document.getElementById('nodesTable');
                    
                    // Keep header, replace content
                    const headerRow = tbody.querySelector('tr');
                    tbody.innerHTML = '';
                    tbody.appendChild(headerRow);
                    
                    if (nodes.length === 0) {{
                        const row = tbody.insertRow();
                        row.innerHTML = '<td colspan="4">No nodes registered</td>';
                        return;
                    }}
                    
                    nodes.forEach(node => {{
                        const row = tbody.insertRow();
                        const brainIcon = node.is_brain ? '🧠 ' : '';
                        const statusClass = `status-${{node.status}}`;
                        
                        row.innerHTML = `
                            <td>
                                <div class="node-name ${{node.is_brain ? 'brain-node' : ''}}">
                                    ${{brainIcon}}${{node.name}}
                                </div>
                            </td>
                            <td><span class="${{statusClass}}">${{node.status}}</span></td>
                            <td>${{node.free_capacity_gb.toFixed(1)}} / ${{node.total_capacity_gb.toFixed(1)}} GB</td>
                            <td>${{node.priority_score.toFixed(2)}}</td>
                        `;
                    }});
                }}
                
                function updateFilesTable(files) {{
                    const tbody = document.getElementById('filesTable');
                    
                    // Keep header, replace content
                    const headerRow = tbody.querySelector('tr');
                    tbody.innerHTML = '';
                    tbody.appendChild(headerRow);
                    
                    if (files.length === 0) {{
                        const row = tbody.insertRow();
                        row.innerHTML = '<td colspan="5">No files uploaded</td>';
                        return;
                    }}
                    
                    files.forEach(file => {{
                        const row = tbody.insertRow();
                        const status = file.complete ? '✓ Complete' : '⏳ Uploading';
                        
                        row.innerHTML = `
                            <td><a href="/file-info/${{file.id}}" style="color: #2196F3; text-decoration: none;">${{file.filename}}</a></td>
                            <td>${{file.size_mb.toFixed(1)}} MB</td>
                            <td>${{file.chunks}}</td>
                            <td>${{status}}</td>
                            <td>
                                <button onclick="checkAndDownload(${{file.id}}, '${{file.filename}}')" style="color: #4CAF50; background: none; border: none; cursor: pointer; font-weight: bold; margin-right: 10px;">⬇ Download</button>
                                <a href="/file-info/${{file.id}}" style="color: #2196F3; text-decoration: none; margin-right: 10px;">ℹ️ Info</a>
                                <button onclick="deleteFile(${{file.id}}, '${{file.filename}}')" style="color: #f44336; background: none; border: none; cursor: pointer; font-weight: bold;">🗑️ Delete</button>
                            </td>
                        `;
                    }});
                }}
                
                async function refreshDashboard() {{
                    try {{
                        showRefreshIndicator();
                        
                        // Fetch all data
                        const [nodesResponse, filesResponse] = await Promise.all([
                            fetch('/api/nodes'),
                            fetch('/api/files')
                        ]);
                        
                        const nodes = await nodesResponse.json();
                        const files = await filesResponse.json();
                        
                        // Calculate stats
                        const totalCapacity = nodes.reduce((sum, n) => sum + n.total_capacity_gb, 0);
                        const freeCapacity = nodes.reduce((sum, n) => sum + n.free_capacity_gb, 0);
                        const usedCapacity = totalCapacity - freeCapacity;
                        const activeNodes = nodes.filter(n => n.status === 'online').length;
                        
                        const stats = {{
                            total_capacity: totalCapacity,
                            used_capacity: usedCapacity,
                            active_nodes: activeNodes,
                            total_files: files.length
                        }};
                        
                        // Update UI
                        updateStats(stats);
                        updateNodesTable(nodes);
                        updateFilesTable(files);
                        
                        lastRefresh = Date.now();
                        
                    }} catch (error) {{
                        console.error('Dashboard refresh failed:', error);
                    }}
                }}
                
                async function checkAndDownload(fileId, filename) {{
                    try {{
                        // Check node status before download
                        const nodesResponse = await fetch('/api/nodes');
                        const nodes = await nodesResponse.json();
                        
                        const onlineNodes = nodes.filter(n => n.status === 'online');
                        const offlineNodes = nodes.filter(n => n.status !== 'online');
                        
                        if (offlineNodes.length > 0) {{
                            let message = `Warning: ${{offlineNodes.length}} node(s) are offline:\n`;
                            offlineNodes.forEach(node => {{
                                message += `- ${{node.name}} (${{node.status}})\n`;
                            }});
                            message += `\nDownload may fail if required chunks are on offline nodes. Continue anyway?`;
                            
                            if (!confirm(message)) {{
                                return;
                            }}
                        }}
                        
                        // Proceed with download
                        window.location.href = `/api/files/${{fileId}}/download`;
                        
                    }} catch (error) {{
                        alert('Error checking node status: ' + error.message);
                    }}
                }}
                
                async function uploadFile(event) {{
                    event.preventDefault();
                    
                    const fileInput = document.getElementById('fileInput');
                    const uploadBtn = document.getElementById('uploadBtn');
                    
                    if (!fileInput.files[0]) return;
                    
                    const file = fileInput.files[0];
                    const fileSizeMB = file.size / (1024 * 1024);
                    
                    const formData = new FormData();
                    formData.append('file', file);
                    formData.append('uploaded_by', 'web-user');
                    
                    const uploadUrl = fileSizeMB > 500 ? '/api/files/upload/stream' : '/api/files/upload';
                    
                    uploadBtn.disabled = true;
                    uploadBtn.textContent = 'Uploading...';
                    
                    try {{
                        const response = await fetch(uploadUrl, {{
                            method: 'POST',
                            body: formData
                        }});
                        
                        if (response.ok) {{
                            const result = await response.json();
                            alert(`Upload complete! File split into ${{result.total_chunks}} chunks.`);
                            fileInput.value = '';
                            await refreshDashboard(); // Refresh to show new file
                        }} else {{
                            alert('Upload failed: ' + response.statusText);
                        }}
                    }} catch (error) {{
                        alert('Upload error: ' + error.message);
                    }} finally {{
                        uploadBtn.disabled = false;
                        uploadBtn.textContent = '📤 Upload to Cluster';
                    }}
                }}
                
                async function deleteFile(fileId, filename) {{
                    if (!confirm(`Are you sure you want to delete "${{filename}}"?\n\nThis will remove the file and all its chunks from all nodes.`)) {{
                        return;
                    }}
                    
                    try {{
                        const response = await fetch(`/api/files/${{fileId}}`, {{
                            method: 'DELETE'
                        }});
                        
                        if (response.ok) {{
                            alert('File deleted successfully!');
                            await refreshDashboard();
                        }} else {{
                            alert('Failed to delete file: ' + response.statusText);
                        }}
                    }} catch (error) {{
                        alert('Error deleting file: ' + error.message);
                    }}
                }}
                
                async function rebalanceCluster() {{
                    const btn = document.getElementById('rebalanceBtn');
                    const statusDiv = document.getElementById('rebalanceStatus');
                    
                    if (!confirm('Rebalance the cluster? This will redistribute chunks across all nodes for optimal balance.')) {{
                        return;
                    }}
                    
                    btn.disabled = true;
                    btn.textContent = 'Analyzing...';
                    
                    try {{
                        const response = await fetch('/api/rebalance/execute', {{
                            method: 'POST',
                            headers: {{ 'Content-Type': 'application/json' }}
                        }});
                        
                        const result = await response.json();
                        
                        if (result.status === 'rebalancing_started') {{
                            alert(`Rebalancing started! ${{result.jobs_created}} migration jobs created.`);
                            statusDiv.style.display = 'block';
                            checkRebalanceStatus();
                        }} else if (result.status === 'no_rebalancing_needed') {{
                            alert('Cluster is already balanced!');
                            btn.disabled = false;
                            btn.textContent = '⚖️ Rebalance Cluster';
                        }}
                    }} catch (error) {{
                        alert('Error starting rebalancing: ' + error.message);
                        btn.disabled = false;
                        btn.textContent = '⚖️ Rebalance Cluster';
                    }}
                }}
                
                async function checkRebalanceStatus() {{
                    const statusDiv = document.getElementById('rebalanceStatus');
                    const progressBar = document.getElementById('rebalanceProgress');
                    const progressText = document.getElementById('rebalanceText');
                    const btn = document.getElementById('rebalanceBtn');
                    
                    try {{
                        const response = await fetch('/api/rebalance/status');
                        const status = await response.json();
                        
                        if (status.is_active) {{
                            progressBar.style.width = status.progress_percent + '%';
                            progressText.textContent = `${{status.completed}} of ${{status.total_jobs}} migrations complete (${{status.progress_percent}}%) - ${{status.pending}} pending, ${{status.in_progress}} in progress`;
                            
                            setTimeout(checkRebalanceStatus, 3000);
                        }} else {{
                            progressBar.style.width = '100%';
                            progressText.textContent = `✓ Rebalancing complete! ${{status.completed}} migrations finished.`;
                            
                            btn.disabled = false;
                            btn.textContent = '⚖️ Rebalance Cluster';
                            
                            setTimeout(() => {{
                                statusDiv.style.display = 'none';
                                refreshDashboard();
                            }}, 5000);
                        }}
                    }} catch (error) {{
                        console.error('Error checking rebalance status:', error);
                    }}
                }}
                
                // Initialize dashboard
                window.addEventListener('load', async () => {{
                    await refreshDashboard();
                    
                    // Check if rebalancing is already active
                    try {{
                        const response = await fetch('/api/rebalance/status');
                        const status = await response.json();
                        
                        if (status.is_active) {{
                            document.getElementById('rebalanceStatus').style.display = 'block';
                            document.getElementById('rebalanceBtn').disabled = true;
                            document.getElementById('rebalanceBtn').textContent = 'Rebalancing...';
                            checkRebalanceStatus();
                        }}
                    }} catch (error) {{
                        console.error('Error checking initial rebalance status:', error);
                    }}
                    
                    // Set up auto-refresh
                    setInterval(refreshDashboard, REFRESH_INTERVAL);
                }});
            </script>
        </body>
        </html>
        """
        
        return html
    
    finally:
        db.close()


@app.get("/api/rebalance/analyze")
async def analyze_cluster_balance():
    """Analyze current cluster balance"""
    db = SessionLocal()
    
    try:
        rebalancer = ClusterRebalancer(db)
        analysis = rebalancer.analyze_distribution()
        
        return analysis
    
    finally:
        db.close()


@app.post("/api/rebalance/plan")
async def create_rebalance_plan(max_migrations: int = 50):
    """Create a rebalancing plan"""
    db = SessionLocal()
    
    try:
        rebalancer = ClusterRebalancer(db)
        migrations = rebalancer.create_rebalancing_plan(max_migrations)
        
        return {
            "migrations": migrations,
            "count": len(migrations)
        }
    
    finally:
        db.close()


@app.post("/api/rebalance/execute")
async def execute_rebalancing(max_migrations: int = 50):
    """Execute cluster rebalancing"""
    db = SessionLocal()
    
    try:
        rebalancer = ClusterRebalancer(db)
        
        # Create plan
        migrations = rebalancer.create_rebalancing_plan(max_migrations)
        
        if not migrations:
            return {
                "status": "no_rebalancing_needed",
                "message": "Cluster is already balanced"
            }
        
        # Execute
        jobs_created = rebalancer.execute_rebalancing(migrations)
        
        print(f"✓ Rebalancing initiated: {jobs_created} migration jobs created")
        
        return {
            "status": "rebalancing_started",
            "migrations_planned": len(migrations),
            "jobs_created": jobs_created
        }
    
    finally:
        db.close()


@app.get("/api/rebalance/status")
async def get_rebalance_status():
    """Get rebalancing progress"""
    db = SessionLocal()
    
    try:
        rebalancer = ClusterRebalancer(db)
        status = rebalancer.get_rebalancing_status()
        
        # Also include current balance score
        analysis = rebalancer.analyze_distribution()
        status["current_balance_score"] = analysis.get("balance_score", 0)
        
        return status
    
    finally:
        db.close()


@app.post("/api/rebalance/complete")
async def rebalance_complete(completion_data: dict):
    """Handle completion of a chunk rebalancing"""
    db = SessionLocal()
    
    try:
        chunk_id = completion_data["chunk_id"]
        new_node_id = completion_data["new_node_id"]
        local_path = completion_data["local_path"]
        
        # Update chunk's primary node
        chunk = db.query(Chunk).filter_by(id=chunk_id).first()
        if not chunk:
            raise HTTPException(status_code=404, detail="Chunk not found")
        
        old_node_id = chunk.primary_node_id
        chunk.primary_node_id = new_node_id
        
        # Create new primary replica record
        new_replica = Replica(
            chunk_id=chunk_id,
            node_id=new_node_id,
            is_primary=True,
            local_path=local_path,
            verification_status="valid"
        )
        db.add(new_replica)
        
        # Update old replica to non-primary (or delete it)
        old_replica = db.query(Replica).filter_by(
            chunk_id=chunk_id,
            node_id=old_node_id,
            is_primary=True
        ).first()
        
        if old_replica:
            old_replica.is_primary = False
            # Optionally schedule deletion of old chunk
        
        db.commit()
        
        print(f"✓ Rebalancing complete: Chunk {chunk_id} moved to node {new_node_id}")
        
        return {"status": "completed", "chunk_id": chunk_id}
    
    except Exception as e:
        db.rollback()
        print(f"✗ Error completing rebalancing: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        db.close()


# ===== WEB PORTAL =====

@app.get("/file-info/{file_id}", response_class=HTMLResponse)
async def file_info_page(file_id: int):
    """Display detailed information about a file"""
    db = SessionLocal()
    
    try:
        file_record = db.query(FileModel).filter_by(id=file_id).first()
        
        if not file_record:
            return HTMLResponse("<h1>File not found</h1>", status_code=404)
        
        chunks = db.query(Chunk).filter_by(file_id=file_id).order_by(Chunk.chunk_index).all()
        
        # Build chunk distribution table
        chunk_rows = []
        nodes_with_file = set()
        
        for chunk in chunks:
            replicas = db.query(Replica).filter_by(chunk_id=chunk.id).all()
            
            replica_info = []
            for replica in replicas:
                node = db.query(Node).filter_by(id=replica.node_id).first()
                if node:
                    nodes_with_file.add(node.name)
                    status_icon = "🟢" if replica.is_primary else "🔵"
                    replica_info.append(f"{status_icon} {node.name}")
            
            chunk_rows.append(f"""
                <tr>
                    <td>{chunk.chunk_index + 1}</td>
                    <td>{chunk.size_bytes / (1024**2):.2f} MB</td>
                    <td>{len(replicas)}</td>
                    <td>{', '.join(replica_info) if replica_info else 'No replicas'}</td>
                </tr>
            """)
        
        nodes_list = ', '.join(sorted(nodes_with_file)) if nodes_with_file else 'None'
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>File Info: {file_record.filename}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: 0 auto; }}
                h1 {{ color: #333; }}
                .info-card {{ background: white; padding: 20px; border-radius: 8px; margin: 20px 0; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .info-row {{ display: flex; padding: 10px 0; border-bottom: 1px solid #eee; }}
                .info-row:last-child {{ border-bottom: none; }}
                .info-label {{ font-weight: bold; width: 200px; color: #666; }}
                .info-value {{ flex: 1; }}
                table {{ width: 100%; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-collapse: collapse; margin-top: 20px; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
                th {{ background: #2196F3; color: white; font-weight: bold; }}
                tr:last-child td {{ border-bottom: none; }}
                .back-link {{ display: inline-block; margin-bottom: 20px; color: #2196F3; text-decoration: none; }}
                .back-link:hover {{ text-decoration: underline; }}
                .legend {{ margin-top: 10px; font-size: 14px; color: #666; }}
                .download-btn {{ display: inline-block; background: #4CAF50; color: white; padding: 10px 20px; border-radius: 4px; text-decoration: none; margin-top: 10px; }}
                .download-btn:hover {{ background: #45a049; }}
            </style>
        </head>
        <body>
            <div class="container">
                <a href="/" class="back-link">← Back to Dashboard</a>
                
                <h1>📄 {file_record.filename}</h1>
                
                <div class="info-card">
                    <h2>File Information</h2>
                    <div class="info-row">
                        <div class="info-label">Filename:</div>
                        <div class="info-value">{file_record.filename}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">Size:</div>
                        <div class="info-value">{file_record.total_size_bytes / (1024**2):.2f} MB ({file_record.total_size_bytes:,} bytes)</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">Chunks:</div>
                        <div class="info-value">{file_record.total_chunks}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">Chunk Size:</div>
                        <div class="info-value">{file_record.chunk_size_bytes / (1024**2):.0f} MB</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">SHA256:</div>
                        <div class="info-value"><code>{file_record.checksum_sha256}</code></div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">Upload Status:</div>
                        <div class="info-value">{'✓ Complete' if file_record.is_complete else '⏳ In Progress'}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">Uploaded By:</div>
                        <div class="info-value">{file_record.uploaded_by or 'Unknown'}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">Upload Date:</div>
                        <div class="info-value">{file_record.created_at.strftime('%Y-%m-%d %H:%M:%S')}</div>
                    </div>
                    <div class="info-row">
                        <div class="info-label">Nodes Storing File:</div>
                        <div class="info-value"><strong>{nodes_list}</strong></div>
                    </div>
                    <a href="/api/files/{file_id}/download" class="download-btn">⬇ Download File</a>
                </div>
                
                <h2>Chunk Distribution</h2>
                <div class="legend">
                    <strong>Legend:</strong> 🟢 Primary replica | 🔵 Secondary replica
                </div>
                <table>
                    <tr>
                        <th>Chunk #</th>
                        <th>Size</th>
                        <th>Replicas</th>
                        <th>Stored On</th>
                    </tr>
                    {''.join(chunk_rows)}
                </table>
            </div>
        </body>
        </html>
        """
        
        return html
    
    finally:
        db.close()


@app.get("/", response_class=HTMLResponse)
async def web_portal():
    """Serve the web portal"""
    db = SessionLocal()
    
    try:
        nodes = db.query(Node).all()
        files = db.query(FileModel).all()
        
        total_capacity = sum(n.total_capacity_gb for n in nodes)
        free_capacity = sum(n.free_capacity_gb for n in nodes)
        used_capacity = total_capacity - free_capacity
        
        # Also calculate based on actual file sizes for accuracy
        actual_used = sum(f.total_size_bytes for f in files if f.is_complete) / (1024**3)
        
        # Use the larger of the two (in case of discrepancies)
        used_capacity = max(used_capacity, actual_used)
        
        nodes_html = "".join([
            f"""<tr>
                <td><strong>{n.name}</strong></td>
                <td><span class="status-{n.status.value}">{n.status.value}</span></td>
                <td>{n.free_capacity_gb:.1f} / {n.total_capacity_gb:.1f} GB</td>
                <td>{n.priority_score:.2f}</td>
            </tr>"""
            for n in nodes
        ])
        
        files_html = "".join([
            f"""<tr>
                <td><a href="/file-info/{f.id}" style="color: #2196F3; text-decoration: none;">{f.filename}</a></td>
                <td>{f.total_size_bytes / (1024**2):.1f} MB</td>
                <td>{f.total_chunks}</td>
                <td>{'✓ Complete' if f.is_complete else '⏳ Uploading'}</td>
                <td>
                    <a href="/api/files/{f.id}/download" style="color: #4CAF50; text-decoration: none; font-weight: bold; margin-right: 10px;">⬇ Download</a>
                    <a href="/file-info/{f.id}" style="color: #2196F3; text-decoration: none; margin-right: 10px;">ℹ️ Info</a>
                    <button onclick="deleteFile({f.id}, '{f.filename}')" style="color: #f44336; background: none; border: none; cursor: pointer; font-weight: bold;">🗑️ Delete</button>
                </td>
            </tr>"""
            for f in files
        ])
        
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>TossIt - Cluster Dashboard</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
                h1 {{ color: #333; }}
                .stats {{ display: flex; gap: 20px; margin: 20px 0; }}
                .stat-box {{ background: white; padding: 20px; border-radius: 8px; flex: 1; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                .stat-box h3 {{ margin: 0 0 10px 0; color: #666; font-size: 14px; }}
                .stat-box .value {{ font-size: 32px; font-weight: bold; color: #2196F3; }}
                table {{ width: 100%; background: white; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); border-collapse: collapse; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #eee; }}
                th {{ background: #2196F3; color: white; font-weight: bold; }}
                tr:last-child td {{ border-bottom: none; }}
                .status-online {{ color: #4CAF50; font-weight: bold; }}
                .status-offline {{ color: #f44336; font-weight: bold; }}
                .section {{ margin: 30px 0; }}
                .upload-section {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 30px; }}
                .upload-form {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
                .file-input {{ padding: 10px; border: 2px solid #ddd; border-radius: 4px; flex: 1; min-width: 250px; }}
                .upload-btn {{ background: #2196F3; color: white; padding: 12px 24px; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; }}
                .upload-btn:hover {{ background: #1976D2; }}
                .upload-btn:disabled {{ background: #ccc; cursor: not-allowed; }}
                #uploadProgress {{ display: none; margin-top: 20px; }}
                .progress-bar {{ width: 100%; height: 30px; background: #f0f0f0; border-radius: 15px; overflow: hidden; margin: 10px 0; position: relative; }}
                .progress-fill {{ height: 100%; background: linear-gradient(90deg, #4CAF50, #8BC34A); transition: width 0.3s; display: flex; align-items: center; justify-content: center; color: white; font-weight: bold; }}
                .upload-stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; margin-top: 15px; }}
                .stat-item {{ background: #f5f5f5; padding: 10px; border-radius: 4px; text-align: center; }}
                .stat-label {{ font-size: 12px; color: #666; margin-bottom: 5px; }}
                .stat-value {{ font-size: 18px; font-weight: bold; color: #2196F3; }}
                .upload-status {{ font-size: 16px; margin-bottom: 10px; color: #333; }}
                .chunk-info {{ background: #e3f2fd; padding: 10px; border-radius: 4px; margin-top: 10px; font-size: 14px; }}
            </style>
        </head>
        <body>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <h1>🗄️ TossIt Cluster Dashboard</h1>
                <div>
                    <a href="/settings" style="background: #666; color: white; padding: 10px 20px; border-radius: 4px; text-decoration: none; font-weight: bold; margin-right: 10px;">⚙️ Settings</a>
                    <button onclick="rebalanceCluster()" id="rebalanceBtn" style="background: #FF9800; color: white; padding: 10px 20px; border-radius: 4px; border: none; cursor: pointer; font-weight: bold;">⚖️ Rebalance Cluster</button>
                </div>
            </div>
            
            <div id="rebalanceStatus" style="display: none; background: #fff3cd; padding: 15px; border-radius: 8px; margin: 20px 0; border-left: 4px solid #FF9800;">
                <strong>Rebalancing in Progress...</strong>
                <div style="margin-top: 10px;">
                    <div style="background: #f0f0f0; height: 20px; border-radius: 10px; overflow: hidden;">
                        <div id="rebalanceProgress" style="background: #4CAF50; height: 100%; width: 0%; transition: width 0.3s;"></div>
                    </div>
                    <p id="rebalanceText" style="margin-top: 5px; font-size: 14px;"></p>
                </div>
            </div>
            
            <div class="stats">
                <div class="stat-box">
                    <h3>Total Capacity</h3>
                    <div class="value">{total_capacity:.1f} GB</div>
                </div>
                <div class="stat-box">
                    <h3>Used Space</h3>
                    <div class="value">{used_capacity:.1f} GB</div>
                </div>
                <div class="stat-box">
                    <h3>Active Nodes</h3>
                    <div class="value">{len([n for n in nodes if n.status == NodeStatus.ONLINE])}</div>
                </div>
                <div class="stat-box">
                    <h3>Total Files</h3>
                    <div class="value">{len(files)}</div>
                </div>
            </div>
            
            <div class="upload-section">
                <h2>Upload File</h2>
                <form id="uploadForm" class="upload-form" onsubmit="uploadFile(event)">
                    <input type="file" id="fileInput" class="file-input" required>
                    <button type="submit" class="upload-btn" id="uploadBtn">📤 Upload to Cluster</button>
                </form>
                <div id="uploadProgress">
                    <div class="upload-status" id="uploadStatus">Preparing upload...</div>
                    <div class="progress-bar">
                        <div class="progress-fill" id="progressFill" style="width: 0%">0%</div>
                    </div>
                    <div class="upload-stats">
                        <div class="stat-item">
                            <div class="stat-label">Speed</div>
                            <div class="stat-value" id="uploadSpeed">0 MB/s</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-label">Uploaded</div>
                            <div class="stat-value" id="uploadedSize">0 MB</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-label">Total Size</div>
                            <div class="stat-value" id="totalSize">0 MB</div>
                        </div>
                        <div class="stat-item">
                            <div class="stat-label">Time Remaining</div>
                            <div class="stat-value" id="timeRemaining">Calculating...</div>
                        </div>
                    </div>
                    <div class="chunk-info" id="chunkInfo"></div>
                </div>
            </div>
            
            <div class="section">
                <h2>Cluster Nodes</h2>
                <table>
                    <tr>
                        <th>Name</th>
                        <th>Status</th>
                        <th>Capacity</th>
                        <th>Priority Score</th>
                    </tr>
                    {nodes_html if nodes_html else '<tr><td colspan="4">No nodes registered</td></tr>'}
                </table>
            </div>
            
            <div class="section">
                <h2>Stored Files</h2>
                <table>
                    <tr>
                        <th>Filename</th>
                        <th>Size</th>
                        <th>Chunks</th>
                        <th>Status</th>
                        <th>Actions</th>
                    </tr>
                    {files_html if files_html else '<tr><td colspan="5">No files uploaded</td></tr>'}
                </table>
            </div>
            
            <script>
                async function uploadFile(event) {{
                    event.preventDefault();
                    
                    const fileInput = document.getElementById('fileInput');
                    const uploadBtn = document.getElementById('uploadBtn');
                    const uploadProgress = document.getElementById('uploadProgress');
                    const uploadStatus = document.getElementById('uploadStatus');
                    const progressFill = document.getElementById('progressFill');
                    const uploadSpeed = document.getElementById('uploadSpeed');
                    const uploadedSize = document.getElementById('uploadedSize');
                    const totalSize = document.getElementById('totalSize');
                    const timeRemaining = document.getElementById('timeRemaining');
                    const chunkInfo = document.getElementById('chunkInfo');
                    
                    if (!fileInput.files[0]) return;
                    
                    const file = fileInput.files[0];
                    const fileSizeMB = file.size / (1024 * 1024);
                    const chunkSizeMB = 64; // Default chunk size
                    const estimatedChunks = Math.ceil(fileSizeMB / chunkSizeMB);
                    
                    const formData = new FormData();
                    formData.append('file', file);
                    formData.append('uploaded_by', 'web-user');
                    
                    // Use streaming endpoint for files larger than 500 MB
                    const useStreaming = fileSizeMB > 500;
                    const uploadUrl = useStreaming ? '/api/files/upload/stream' : '/api/files/upload';
                    
                    if (useStreaming) {{
                        uploadStatus.textContent = 'Streaming large file: ' + file.name + '...';
                        chunkInfo.textContent = `📦 Large file detected! Using streaming upload to handle ${{estimatedChunks}} chunks efficiently`;
                    }} else {{
                        uploadStatus.textContent = 'Uploading ' + file.name + '...';
                        chunkInfo.textContent = `📦 This file will be split into approximately ${{estimatedChunks}} chunk${{estimatedChunks > 1 ? 's' : ''}} and distributed across the cluster`;
                    }}
                    
                    uploadBtn.disabled = true;
                    uploadProgress.style.display = 'block';
                    totalSize.textContent = fileSizeMB.toFixed(2) + ' MB';
                    
                    const startTime = Date.now();
                    let lastLoaded = 0;
                    let lastTime = startTime;
                    
                    try {{
                        const xhr = new XMLHttpRequest();
                        
                        xhr.upload.addEventListener('progress', (e) => {{
                            if (e.lengthComputable) {{
                                const percentComplete = (e.loaded / e.total) * 100;
                                const loadedMB = e.loaded / (1024 * 1024);
                                
                                // Update progress bar
                                progressFill.style.width = percentComplete + '%';
                                progressFill.textContent = percentComplete.toFixed(1) + '%';
                                
                                // Update uploaded size
                                uploadedSize.textContent = loadedMB.toFixed(2) + ' MB';
                                
                                // Calculate speed (MB/s)
                                const currentTime = Date.now();
                                const timeDiff = (currentTime - lastTime) / 1000; // seconds
                                const loadedDiff = (e.loaded - lastLoaded) / (1024 * 1024); // MB
                                
                                if (timeDiff > 0.5) {{ // Update every 500ms
                                    const speed = loadedDiff / timeDiff;
                                    uploadSpeed.textContent = speed.toFixed(2) + ' MB/s';
                                    
                                    // Calculate time remaining
                                    const remainingMB = (e.total - e.loaded) / (1024 * 1024);
                                    const remainingSeconds = speed > 0 ? remainingMB / speed : 0;
                                    
                                    if (remainingSeconds < 60) {{
                                        timeRemaining.textContent = Math.ceil(remainingSeconds) + ' sec';
                                    }} else {{
                                        const minutes = Math.floor(remainingSeconds / 60);
                                        const seconds = Math.ceil(remainingSeconds % 60);
                                        timeRemaining.textContent = minutes + ' min ' + seconds + ' sec';
                                    }}
                                    
                                    lastLoaded = e.loaded;
                                    lastTime = currentTime;
                                }}
                            }}
                        }});
                        
                        xhr.addEventListener('load', () => {{
                            if (xhr.status === 200) {{
                                const response = JSON.parse(xhr.responseText);
                                const elapsed = (Date.now() - startTime) / 1000;
                                const avgSpeed = (fileSizeMB / elapsed).toFixed(2);
                                
                                uploadStatus.textContent = '✓ Upload complete!';
                                progressFill.style.width = '100%';
                                progressFill.textContent = '100%';
                                uploadSpeed.textContent = avgSpeed + ' MB/s (avg)';
                                timeRemaining.textContent = 'Complete!';
                                
                                chunkInfo.textContent = `✓ File split into ${{response.total_chunks}} chunk${{response.total_chunks > 1 ? 's' : ''}} and distributed across the cluster in ${{elapsed.toFixed(1)}} seconds`;
                                
                                setTimeout(() => {{
                                    location.reload();
                                }}, 2000);
                            }} else {{
                                uploadStatus.textContent = '✗ Upload failed: ' + xhr.statusText;
                                uploadBtn.disabled = false;
                            }}
                        }});
                        
                        xhr.addEventListener('error', () => {{
                            uploadStatus.textContent = '✗ Upload error: Network error';
                            uploadBtn.disabled = false;
                        }});
                        
                        xhr.open('POST', uploadUrl);
                        xhr.send(formData);
                        
                    }} catch (error) {{
                        uploadStatus.textContent = '✗ Upload error: ' + error.message;
                        uploadBtn.disabled = false;
                    }}
                }}
                
                async function deleteFile(fileId, filename) {{
                    if (!confirm('Are you sure you want to delete "' + filename + '"?\\n\\nThis will remove the file and all its chunks from all nodes.')) {{
                        return;
                    }}
                    
                    try {{
                        const response = await fetch('/api/files/' + fileId, {{
                            method: 'DELETE'
                        }});
                        
                        if (response.ok) {{
                            alert('File deleted successfully!');
                            location.reload();
                        }} else {{
                            alert('Failed to delete file: ' + response.statusText);
                        }}
                    }} catch (error) {{
                        alert('Error deleting file: ' + error.message);
                    }}
                }}
            </script>
        </body>
        </html>
        """
        
        return html
    
    finally:
        db.close()


if __name__ == "__main__":
    import uvicorn
    local_ip = get_local_ip()
    print(f"Starting TossIt Brain Server on {local_ip}:8000...")
    print(f"Dashboard will be available at: http://{local_ip}:8000")
    uvicorn.run(app, host=local_ip, port=8000)