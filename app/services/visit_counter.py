from typing import Dict, List, Any
import asyncio
from datetime import datetime
from ..core.redis_manager import RedisManager



class VisitCounterService:
    def __init__(self):
        """Initialize the visit counter service with an in-memory store"""
        self.visit_counts: Dict[str, int] = {}  # In-memory dictionary

    #     def __init__(self):
    #         """Initialize the visit counter service with Redis manager"""
    #         self.redis_manager = RedisManager()
    
    async def increment_visit(self, page_id: str) -> None:
        """
        Increment visit count for a page.
        
        Args:
            page_id: Unique identifier for the page
        """
        self.visit_counts[page_id] = self.visit_counts.get(page_id, 0) + 1  # Increment count

    async def get_visit_count(self, page_id: str) -> int:
        """
        Get current visit count for a page.
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Current visit count
        """
        return self.visit_counts.get(page_id, 0)  # Return count (default 0 if not found)
