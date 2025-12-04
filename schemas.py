from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, List

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

# ---------- Calculated Fields---------------
class CalculatedFieldBase(BaseModel):
    analysis_id: int
    field_name: str
    formula: str
    default_agg: str | None = None

class CalculatedFieldCreate(CalculatedFieldBase):
    pass

class CalculatedFieldOut(CalculatedFieldBase):
    id: int
    class Config:
        from_attributes = True


class ValueConfig(BaseModel):
    column: str
    agg: str = "sum"

class AnalysisPreviewRequest(BaseModel):
    dataset_id: int
    analysis_id: int
    type: str = "pivot"
    rows: Optional[List[str]] = []
    columns: Optional[List[str]] = []
    values: Optional[List[ValueConfig]] = []

# ----------- Filters ----------
class FilterSaveRequest(BaseModel):
    dataset_id: str
    analysis_id: int
    selected_columns: List[str]

class FilterResponse(BaseModel):
    id: int
    dataset_id: str
    analysis_id: int
    selected_columns: List[str]

    class Config:
        from_attributes = True
