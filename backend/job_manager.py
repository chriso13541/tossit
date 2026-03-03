"""
TossIt Job Manager
Handles background job processing, cleanup, and replication with improved reliability
"""

import asyncio
import aiohttp
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import and_, or_
from typing import Optional, List
import hashlib
import logging

from models import (
    Node, Chunk, Replica, Job, File,
    NodeStatus, JobStatus, JobType
)

logger = logging.getLogger(__name__)


class JobManager:
    """Manages background job processing with automatic cleanup and retry logic"""
    
    def __init__(self, session_factory: sessionmaker):
        self.session_factory = session_factory
        self.is_processing = False
        self.stats = {
            'processed': 0,
            'succeeded': 0,
            'failed': 0,
            'retried': 0,
            'cleaned': 0
        }
        
    async def start_background_processor(self, interval: int = 5):
        """Start continuous background job processing"""
        print("🔄 Starting background job processor")
        
        while True:
            try:
                await self.process_jobs()
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"Error in background processor: {e}")
                await asyncio.sleep(interval)
    
    async def process_jobs(self):
        """Process pending jobs with proper error handling and retry logic"""
        if self.is_processing:
            return  # Avoid concurrent processing
        
        self.is_processing = True
        db = self.session_factory()
        
        try:
            # Get pending jobs ordered by priority
            jobs = db.query(Job).filter(
                Job.status == JobStatus.PENDING
            ).order_by(Job.priority.desc()).limit(10).all()
            
            if not jobs:
                return
            
            print(f"🔄 Processing {len(jobs)} pending jobs")
            
            for job in jobs:
                try:
                    # Mark as in progress
                    job.status = JobStatus.IN_PROGRESS
                    job.started_at = datetime.utcnow()
                    db.commit()
                    
                    # Process based on job type
                    if job.job_type == JobType.REPLICATION:
                        success = await self._process_replication_job(job, db)
                    elif job.job_type == JobType.VERIFICATION:
                        success = await self._process_verification_job(job, db)
                    elif job.job_type == JobType.REBALANCE:
                        success = await self._process_rebalance_job(job, db)
                    elif job.job_type == JobType.DELETION:
                        success = await self._process_deletion_job(job, db)
                    else:
                        logger.warning(f"Unknown job type: {job.job_type}")
                        success = False
                    
                    if success:
                        job.status = JobStatus.COMPLETED
                        job.completed_at = datetime.utcnow()
                        job.progress_percent = 100.0
                        self.stats['succeeded'] += 1
                    else:
                        # Handle failure with retry logic
                        job.retry_count += 1
                        
                        if job.retry_count >= job.max_retries:
                            job.status = JobStatus.FAILED
                            job.completed_at = datetime.utcnow()
                            self.stats['failed'] += 1
                            logger.error(f"Job {job.id} failed after {job.retry_count} retries")
                        else:
                            job.status = JobStatus.PENDING  # Retry later
                            self.stats['retried'] += 1
                            logger.info(f"Job {job.id} will retry ({job.retry_count}/{job.max_retries})")
                    
                    db.commit()
                    self.stats['processed'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing job {job.id}: {e}")
                    job.status = JobStatus.FAILED
                    job.error_message = str(e)
                    job.completed_at = datetime.utcnow()
                    db.commit()
                    self.stats['failed'] += 1
            
            # Cleanup old jobs periodically
            await self._periodic_cleanup(db)
            
        finally:
            self.is_processing = False
            db.close()
    
    async def _process_replication_job(self, job: Job, db: Session) -> bool:
        """Process a replication job by copying chunk from source to target"""
        try:
            # Get chunk
            chunk = db.query(Chunk).filter(Chunk.id == job.chunk_id).first()
            if not chunk:
                job.error_message = "Chunk not found"
                return False
            
            # Get source node (where chunk currently exists)
            source_node = db.query(Node).filter(Node.id == job.source_node_id).first()
            if not source_node or source_node.status != NodeStatus.ONLINE:
                job.error_message = "Source node offline or not found"
                return False
            
            # Get target node (where we want to replicate)
            target_node = db.query(Node).filter(Node.id == job.target_node_id).first()
            if not target_node or target_node.status != NodeStatus.ONLINE:
                job.error_message = "Target node offline or not found"
                return False
            
            # Check if replica already exists
            existing_replica = db.query(Replica).filter(
                Replica.chunk_id == chunk.id,
                Replica.node_id == target_node.id
            ).first()
            
            if existing_replica:
                print(f"✓ Replica already exists for chunk {chunk.id} on {target_node.name} (job {job.id} completing)")
                # Mark job as complete since replica exists
                job.status = JobStatus.COMPLETED
                job.completed_at = datetime.utcnow()
                job.progress_percent = 100.0
                db.commit()
                return True
            
            # Download chunk from source
            async with aiohttp.ClientSession() as session:
                source_url = f"http://{source_node.ip_address}:{source_node.port}/api/chunks/{chunk.id}/data"
                
                async with session.get(source_url, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status != 200:
                        job.error_message = f"Failed to download from source: {resp.status}"
                        return False
                    
                    chunk_data = await resp.read()
                
                # Verify checksum
                chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                if chunk_hash != chunk.checksum_sha256:
                    job.error_message = "Checksum verification failed"
                    return False
                
                job.progress_percent = 50.0
                db.commit()
                
                # Upload to target
                target_url = f"http://{target_node.ip_address}:{target_node.port}/api/chunks/store"
                data = aiohttp.FormData()
                data.add_field('chunk_id', str(chunk.id))
                data.add_field('chunk_data', chunk_data, filename=f'chunk_{chunk.id}.dat')
                
                async with session.post(target_url, data=data, timeout=aiohttp.ClientTimeout(total=120)) as resp:
                    if resp.status != 200:
                        job.error_message = f"Failed to upload to target: {resp.status}"
                        return False
                
                job.progress_percent = 90.0
                db.commit()
            
            # Create replica record
            replica = Replica(
                chunk_id=chunk.id,
                node_id=target_node.id,
                is_primary=False,
                local_path=f"/chunks/chunk_{chunk.id}.dat",
                verification_status="valid"
            )
            db.add(replica)
            
            # Update target node capacity
            target_node.free_capacity_gb -= chunk.size_bytes / (1024**3)
            
            db.commit()
            
            print(f"✅ Replicated chunk {chunk.id} from {source_node.name} to {target_node.name}")
            
            # Check if file is now fully replicated
            self._update_file_replication_status(chunk.file_id, db)
            
            return True
            
        except Exception as e:
            logger.error(f"Replication job error: {e}")
            job.error_message = str(e)
            return False
    
    async def _process_verification_job(self, job: Job, db: Session) -> bool:
        """Verify chunk integrity by checking checksums"""
        try:
            chunk = db.query(Chunk).filter(Chunk.id == job.chunk_id).first()
            if not chunk:
                return False
            
            replicas = db.query(Replica).filter(Replica.chunk_id == chunk.id).all()
            
            verified_count = 0
            corrupted_replicas = []
            
            for replica in replicas:
                node = db.query(Node).filter(Node.id == replica.node_id).first()
                if not node or node.status != NodeStatus.ONLINE:
                    continue
                
                try:
                    async with aiohttp.ClientSession() as session:
                        url = f"http://{node.ip_address}:{node.port}/api/chunks/{chunk.id}/data"
                        async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                            if resp.status == 200:
                                chunk_data = await resp.read()
                                chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                                
                                if chunk_hash == chunk.checksum_sha256:
                                    replica.verification_status = "valid"
                                    replica.last_verified = datetime.utcnow()
                                    verified_count += 1
                                else:
                                    replica.verification_status = "corrupted"
                                    corrupted_replicas.append(replica.id)
                                    logger.warning(f"Corrupted replica detected: chunk {chunk.id} on {node.name}")
                except Exception as e:
                    logger.error(f"Verification failed for replica on {node.name}: {e}")
            
            db.commit()
            
            # Schedule re-replication for corrupted replicas
            if corrupted_replicas:
                logger.warning(f"Found {len(corrupted_replicas)} corrupted replicas, scheduling re-replication")
                # TODO: Create replication jobs to replace corrupted replicas
            
            return verified_count > 0
            
        except Exception as e:
            logger.error(f"Verification job error: {e}")
            return False
    
    async def _process_rebalance_job(self, job: Job, db: Session) -> bool:
        """Move chunk from source to target node for load balancing"""
        try:
            # Similar to replication but also removes from source
            chunk = db.query(Chunk).filter(Chunk.id == job.chunk_id).first()
            if not chunk:
                return False
            
            # First replicate to target
            success = await self._process_replication_job(job, db)
            if not success:
                return False
            
            # Then update primary if needed
            if chunk.primary_node_id == job.source_node_id:
                chunk.primary_node_id = job.target_node_id
                
                # Update replica flags
                old_primary = db.query(Replica).filter(
                    Replica.chunk_id == chunk.id,
                    Replica.node_id == job.source_node_id
                ).first()
                if old_primary:
                    old_primary.is_primary = False
                
                new_primary = db.query(Replica).filter(
                    Replica.chunk_id == chunk.id,
                    Replica.node_id == job.target_node_id
                ).first()
                if new_primary:
                    new_primary.is_primary = True
            
            db.commit()
            
            print(f"✅ Rebalanced chunk {chunk.id} from node {job.source_node_id} to {job.target_node_id}")
            return True
            
        except Exception as e:
            logger.error(f"Rebalance job error: {e}")
            return False
    
    async def _process_deletion_job(self, job: Job, db: Session) -> bool:
        """Delete chunk from a specific node"""
        try:
            node = db.query(Node).filter(Node.id == job.target_node_id).first()
            if not node or node.status != NodeStatus.ONLINE:
                return False
            
            async with aiohttp.ClientSession() as session:
                url = f"http://{node.ip_address}:{node.port}/delete_chunk/{job.chunk_id}"
                async with session.delete(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        # Remove replica record
                        db.query(Replica).filter(
                            Replica.chunk_id == job.chunk_id,
                            Replica.node_id == job.target_node_id
                        ).delete()
                        db.commit()
                        return True
            
            return False
            
        except Exception as e:
            logger.error(f"Deletion job error: {e}")
            return False
    
    def _update_file_replication_status(self, file_id: int, db: Session):
        """Check if all chunks of a file are properly replicated"""
        from tossit_settings import get_settings
        
        settings = get_settings(db)
        min_replicas = settings.min_replicas
        
        chunks = db.query(Chunk).filter(Chunk.file_id == file_id).all()
        all_replicated = True
        
        for chunk in chunks:
            replica_count = db.query(Replica).filter(Replica.chunk_id == chunk.id).count()
            if replica_count < min_replicas:
                all_replicated = False
                break
        
        if all_replicated:
            file_record = db.query(File).filter(File.id == file_id).first()
            if file_record and not file_record.is_replicated:
                file_record.is_replicated = True
                db.commit()
                print(f"✅ File {file_id} is now fully replicated")
    
    async def _periodic_cleanup(self, db: Session):
        """Clean up old completed/failed jobs"""
        # Run cleanup every 100 processed jobs
        if self.stats['processed'] % 100 == 0:
            cleaned = self.cleanup_old_jobs(db)
            if cleaned > 0:
                print(f"🧹 Cleaned up {cleaned} old jobs")
    
    def cleanup_old_jobs(self, db: Session = None) -> int:
        """Remove old completed and failed jobs"""
        should_close = False
        if db is None:
            db = self.session_factory()
            should_close = True
        
        try:
            # Remove completed jobs older than 24 hours
            completed_cutoff = datetime.utcnow() - timedelta(hours=24)
            completed_deleted = db.query(Job).filter(
                and_(
                    Job.status == JobStatus.COMPLETED,
                    Job.completed_at < completed_cutoff
                )
            ).delete(synchronize_session=False)
            
            # Remove failed jobs older than 7 days
            failed_cutoff = datetime.utcnow() - timedelta(days=7)
            failed_deleted = db.query(Job).filter(
                and_(
                    Job.status == JobStatus.FAILED,
                    Job.completed_at < failed_cutoff
                )
            ).delete(synchronize_session=False)
            
            db.commit()
            
            total_cleaned = completed_deleted + failed_deleted
            self.stats['cleaned'] += total_cleaned
            
            return total_cleaned
            
        finally:
            if should_close:
                db.close()
    
    def get_status(self) -> dict:
        """Get current job manager status"""
        db = self.session_factory()
        try:
            pending = db.query(Job).filter(Job.status == JobStatus.PENDING).count()
            in_progress = db.query(Job).filter(Job.status == JobStatus.IN_PROGRESS).count()
            completed = db.query(Job).filter(Job.status == JobStatus.COMPLETED).count()
            failed = db.query(Job).filter(Job.status == JobStatus.FAILED).count()
            
            return {
                "is_processing": self.is_processing,
                "pending_jobs": pending,
                "in_progress_jobs": in_progress,
                "completed_jobs": completed,
                "failed_jobs": failed,
                "stats": self.stats.copy()
            }
        finally:
            db.close()