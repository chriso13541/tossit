"""
TossIt Upload Queue Manager
Handles concurrent upload throttling and queue management for high-load scenarios
"""

import asyncio
from asyncio import Queue
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import time
import uuid
from typing import Dict, List, Tuple, Optional

@dataclass
class UploadRequest:
    session_id: str
    filename: str
    file_size: int
    user: str
    timestamp: datetime
    retry_count: int = 0

class UploadQueueManager:
    def __init__(self, max_concurrent_uploads: int = None, cpu_cores: int = 4):
        # Auto-detect optimal concurrency based on system resources
        if max_concurrent_uploads is None:
            # Base on CPU cores, database connections, and memory
            max_concurrent_uploads = min(max(2, cpu_cores), 20)  # 2-20 range
        
        self.max_concurrent = max_concurrent_uploads
        self.upload_queue = Queue(maxsize=1000)  # Queue up to 1000 requests
        self.active_uploads: Dict[str, dict] = {}  # session_id -> upload_info
        self.upload_semaphore = asyncio.Semaphore(max_concurrent_uploads)
        self.queue_processor_task: Optional[asyncio.Task] = None
        self.stats = {
            'queued': 0,
            'active': 0,
            'completed': 0,
            'failed': 0,
            'queue_overflows': 0,
            'database_errors': 0,
            'node_errors': 0
        }
        
        # Adaptive throttling
        self.error_rate_window = timedelta(minutes=5)
        self.recent_errors: List[Tuple[datetime, str]] = []
        self.throttle_factor = 1.0  # Multiplier for delays
        
        print(f"🚦 Upload Queue Manager initialized")
        print(f"   Max concurrent uploads: {self.max_concurrent}")
        print(f"   Queue capacity: 1000 requests")
        print(f"   CPU cores: {cpu_cores}")
    
    async def queue_upload(self, filename: str, file_size: int, user: str) -> str:
        """Queue an upload request"""
        session_id = str(uuid.uuid4())
        
        # Check if queue is full
        if self.upload_queue.full():
            self.stats['queue_overflows'] += 1
            # Try to make space by checking for expired requests
            await self._cleanup_expired_requests()
            
            if self.upload_queue.full():
                from fastapi import HTTPException
                raise HTTPException(
                    status_code=503, 
                    detail=f"Upload queue is full ({self.upload_queue.qsize()}/1000). Please try again in a few minutes."
                )
        
        upload_request = UploadRequest(
            session_id=session_id,
            filename=filename,
            file_size=file_size,
            user=user,
            timestamp=datetime.now(timezone.utc)
        )
        
        await self.upload_queue.put(upload_request)
        self.stats['queued'] += 1
        
        queue_position = self.upload_queue.qsize()
        estimated_wait = queue_position * 2  # Rough estimate: 2 seconds per upload ahead
        
        print(f"📋 Queued upload: {filename} (position {queue_position}, ~{estimated_wait}s wait)")
        
        return session_id
    
    async def _cleanup_expired_requests(self):
        """Remove expired requests from active uploads"""
        current_time = time.time()
        expired_sessions = []
        
        for session_id, upload_info in self.active_uploads.items():
            # Consider uploads expired after 5 minutes
            if current_time - upload_info['start_time'] > 300:
                expired_sessions.append(session_id)
        
        for session_id in expired_sessions:
            del self.active_uploads[session_id]
            print(f"🧹 Cleaned expired upload session: {session_id}")
    
    def get_queue_status(self) -> dict:
        """Get current queue status"""
        return {
            'queue_size': self.upload_queue.qsize(),
            'active_uploads': len(self.active_uploads),
            'max_concurrent': self.max_concurrent,
            'throttle_factor': self.throttle_factor,
            'stats': self.stats.copy(),
            'recent_error_count': len(self.recent_errors),
            'estimated_wait_seconds': self.upload_queue.qsize() * 2
        }
    
    def is_overloaded(self) -> bool:
        """Check if system is currently overloaded"""
        queue_overloaded = self.upload_queue.qsize() > 500  # More than 500 in queue
        error_overloaded = len(self.recent_errors) > 20  # More than 20 recent errors
        throttle_overloaded = self.throttle_factor > 2.0  # High throttling factor
        
        return queue_overloaded or error_overloaded or throttle_overloaded
    
    def get_overload_reason(self) -> str:
        """Get reason for overload"""
        reasons = []
        if self.upload_queue.qsize() > 500:
            reasons.append(f"Queue backlog: {self.upload_queue.qsize()}/1000")
        if len(self.recent_errors) > 20:
            reasons.append(f"High error rate: {len(self.recent_errors)} errors")
        if self.throttle_factor > 2.0:
            reasons.append(f"System throttling: {self.throttle_factor:.1f}x")
        
        return "; ".join(reasons) if reasons else "No overload detected"
    
    def record_upload_start(self, session_id: str):
        """Record that an upload has started processing"""
        self.active_uploads[session_id] = {
            'start_time': time.time(),
            'status': 'processing'
        }
        self.stats['active'] += 1
    
    def record_upload_complete(self, session_id: str, success: bool = True):
        """Record that an upload has completed"""
        if session_id in self.active_uploads:
            del self.active_uploads[session_id]
        
        self.stats['active'] = max(0, self.stats['active'] - 1)
        
        if success:
            self.stats['completed'] += 1
        else:
            self.stats['failed'] += 1
    
    def record_error(self, error: Exception):
        """Record error for adaptive throttling"""
        error_time = datetime.now(timezone.utc)
        error_type = type(error).__name__
        
        self.recent_errors.append((error_time, error_type))
        
        # Count error types for stats
        if 'database' in str(error).lower() or 'sqlite' in str(error).lower():
            self.stats['database_errors'] += 1
        elif 'node' in str(error).lower() or 'connection' in str(error).lower():
            self.stats['node_errors'] += 1
        
        # Clean old errors
        cutoff = error_time - self.error_rate_window
        self.recent_errors = [(t, e) for t, e in self.recent_errors if t > cutoff]
        
        # Update throttle factor
        self._update_throttle_factor()
    
    def _update_throttle_factor(self):
        """Update throttling factor based on recent error rates"""
        if not self.recent_errors:
            self.throttle_factor = 1.0
            return
        
        # Calculate error rate
        error_count = len(self.recent_errors)
        error_rate = error_count / (self.error_rate_window.total_seconds() / 60)  # errors per minute
        
        # Adjust throttling based on error rate
        if error_rate > 10:  # More than 10 errors per minute
            self.throttle_factor = 3.0
        elif error_rate > 5:  # More than 5 errors per minute
            self.throttle_factor = 2.0
        elif error_rate > 2:  # More than 2 errors per minute
            self.throttle_factor = 1.5
        else:
            self.throttle_factor = 1.0
    
    async def wait_if_overloaded(self):
        """Wait if system is overloaded"""
        if self.is_overloaded():
            wait_time = min(self.throttle_factor * 2.0, 10.0)  # Max 10 second wait
            print(f"🐌 System overloaded, waiting {wait_time:.1f}s: {self.get_overload_reason()}")
            await asyncio.sleep(wait_time)