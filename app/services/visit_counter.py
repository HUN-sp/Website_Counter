from typing import Dict, Any
from datetime import datetime

class VisitCounterService:
    def __init__(self):  # Fixed: Changed _init_ to __init__
        """Initialize the visit counter service with an in-memory dictionary"""
        self._visit_counts: Dict[str, int] = {}  # In-memory storage for visit counts

    async def increment_visit(self, page_id: str) -> Dict[str, Any]:
        """
        Increment visit count for a page and return the response
        
        Args:
            page_id: Unique identifier for the page
            
        Returns:
            Dictionary containing visit count and source
        """
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
        count = self._visit_counts.get(page_id, 0)
        return {
            "visits": count,
            "served_via": "in_memory"
        }