from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
from ....services.visit_counter import VisitCounterService
from ....schemas.counter import VisitCount

router = APIRouter()

# Create a single instance of VisitCounterService to maintain state
_visit_counter_service = None

# Dependency to get VisitCounterService instance
def get_visit_counter_service():
    global _visit_counter_service
    if _visit_counter_service is None:
        _visit_counter_service = VisitCounterService()
    return _visit_counter_service

@router.post("/visit/{page_id}", response_model=VisitCount)
async def record_visit(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Record a visit for a page and return the updated count"""
    try:
        result = await counter_service.increment_visit(page_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}", response_model=VisitCount)
async def get_visits(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Get visit count for a page"""
    try:
        result = await counter_service.get_visit_count(page_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))