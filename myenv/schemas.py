from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict

class DatasetMetadataCreate(BaseModel):
    dataset_name: str
    s3_bucket: str
    s3_key: str   
    num_rows: Optional[int] = None   
    num_columns: Optional[int] = None

class DatasetMetadataResponse(DatasetMetadataCreate):
    id: int
    latest_file: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ---------------- Analysis Create ----------------
class AnalysisCreate(BaseModel):
    dataset_id: int
    analysis_name: str
    analysis_type: str  # e.g., "pivot", "bar", "line"
    config: Optional[Dict] = {}  # JSON object storing rows/columns/measures etc.

# ---------------- Analysis Response ----------------
class AnalysisResponse(AnalysisCreate):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True