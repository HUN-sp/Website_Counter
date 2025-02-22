from pydantic import BaseModel
from typing import Dict, Any

class VisitCount(BaseModel):
    visits: int
    served_via: str