from typing import Dict, Any
from datetime import datetime
import redis
import time
import asyncio
import logging

class VisitCounterService:
    _instance = None
    _initialized = False

    def __init__(self):
        """Initialize the visit counter service"""
        if VisitCounterService._initialized:
            return

        # Task 1: In-memory counter
        self._visit_counts: Dict[str, int] = {}
        
        # Task 2: Redis connection
        self.redis_client = redis.Redis(
            host='redis1',
            port=6379,
            decode_responses=True
        )
        
        # Task 3: Application cache
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 5  # 5 seconds TTL
        
        # Task 4: Write buffer
        self._write_buffer: Dict[str, int] = {}  # {page_id: pending_count}
        self.flush_interval = 30  # 30 seconds
        self.is_buffer_flushing = False
        
        self.use_redis = True  # Set to True for Tasks 2,3,4
        
        # Initialize but don't start the flush task yet
        self._flush_task = None
        VisitCounterService._initialized = True

    @classmethod
    async def get_instance(cls):
        """Get or create a singleton instance"""
        if not cls._instance:
            cls._instance = cls()
            # Start the background task only once when first accessed
            if not cls._instance._flush_task:
                cls._instance._flush_task = asyncio.create_task(cls._instance._periodic_flush())
        return cls._instance

    async def _periodic_flush(self):
        """Background task to periodically flush buffer to Redis"""
        while True:
            await asyncio.sleep(self.flush_interval)
            await self._flush_buffer()

    async def _flush_buffer(self):
        """Flush buffered counts to Redis"""
        if self.is_buffer_flushing or not self._write_buffer:
            return

        try:
            self.is_buffer_flushing = True
            buffer_to_flush = self._write_buffer.copy()
            self._write_buffer.clear()

            # Use Redis pipeline for atomic updates
            pipeline = self.redis_client.pipeline()
            for page_id, count in buffer_to_flush.items():
                if count > 0:
                    pipeline.incrby(f"page:{page_id}", count)
            pipeline.execute()

            # Update cache with new values
            for page_id in buffer_to_flush:
                current_count = int(self.redis_client.get(f"page:{page_id}") or 0)
                self._cache[page_id] = {
                    'count': current_count,
                    'timestamp': time.time()
                }

        except Exception as e:
            # Restore buffer on error
            for page_id, count in buffer_to_flush.items():
                self._write_buffer[page_id] = self._write_buffer.get(page_id, 0) + count
            logging.error(f"Error flushing buffer: {str(e)}")
        finally:
            self.is_buffer_flushing = False

    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """Increment visit count for a page"""
        if not self.use_redis:
            # Task 1: In-memory implementation
            if page_id not in self._visit_counts:
                self._visit_counts[page_id] = 0
            self._visit_counts[page_id] += 1
            return {
                "visits": self._visit_counts[page_id],
                "served_via": "in_memory"
            }

        # Add to write buffer
        self._write_buffer[page_id] = self._write_buffer.get(page_id, 0) + 1
        
        # Get current total (Redis + buffer)
        redis_count = int(self.redis_client.get(f"page:{page_id}") or 0)
        buffer_count = self._write_buffer.get(page_id, 0)
        total_count = redis_count + buffer_count
        
        # Update cache with combined count
        self._cache[page_id] = {
            'count': total_count,
            'timestamp': time.time()
        }
        
        return {
            "visits": total_count,
            "served_via": "redis"
        }

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """Get current visit count for a page"""
        if not self.use_redis:
            # Task 1: In-memory implementation
            count = self._visit_counts.get(page_id, 0)
            return {
                "visits": count,
                "served_via": "in_memory"
            }

        # Check cache first (Task 3)
        cached_data = self._cache.get(page_id)
        current_time = time.time()
        
        if cached_data and (current_time - cached_data['timestamp']) < self.cache_ttl:
            # Cache hit
            return {
                "visits": cached_data['count'],
                "served_via": "in_memory"
            }
        
        # Cache miss: Flush buffer and get fresh count
        await self._flush_buffer()
        
        # Get count from Redis
        redis_count = int(self.redis_client.get(f"page:{page_id}") or 0)
        buffer_count = self._write_buffer.get(page_id, 0)
        total_count = redis_count + buffer_count
        
        # Update cache
        self._cache[page_id] = {
            'count': total_count,
            'timestamp': current_time
        }
        
        return {
            "visits": total_count,
            "served_via": "redis"
        }