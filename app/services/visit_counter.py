from typing import Dict, Any
from datetime import datetime
import redis

class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with Redis connection"""
        # Keep in-memory counter for Task 1
        self._visit_counts: Dict[str, int] = {}
        
        # Redis connection for Task 2
        self.redis_client = redis.Redis(
            host='redis1',  # matches the service name in docker-compose
            port=6379,
            decode_responses=True
        )
        
        # Flag to determine which storage to use
        self.use_redis = True  # Set to True for Task 2

    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """
        Increment visit count for a page and return the response
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Dictionary containing visit count and source
        """
        if self.use_redis:
            # Use Redis for storage
            count = self.redis_client.incr(f"page:{page_id}")
            return {
                "visits": count,
                "served_via": "redis"
            }
        else:
            # Use in-memory storage (Task 1)
            if page_id not in self._visit_counts:
                self._visit_counts[page_id] = 0
            self._visit_counts[page_id] += 1
            return {
                "visits": self._visit_counts[page_id],
                "served_via": "in_memory"
            }

    async def get_visit_count(self, page_id: str) -> Dict[str, Any]:
        """
        Get current visit count for a page
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Dictionary containing visit count and source
        """
        if self.use_redis:
            # Get count from Redis
            count = self.redis_client.get(f"page:{page_id}")
            return {
                "visits": int(count) if count else 0,
                "served_via": "redis"
            }
        else:
            # Get from in-memory (Task 1)
            count = self._visit_counts.get(page_id, 0)
            return {
                "visits": count,
                "served_via": "in_memory"
            }