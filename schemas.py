# schemas.py
from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, List, Any

# Dataset
class DatasetMetadataCreate(BaseModel):
    dataset_name: str
    s3_bucket: str
    s3_key: str

class DatasetMetadataResponse(DatasetMetadataCreate):
    id: int
    latest_file: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    class Config:
        from_attributes = True

# Analysis
class AnalysisCreate(BaseModel):
    dataset_id: int
    analysis_name: str
    analysis_type: str
    config: Optional[Dict[str, Any]] = {}

class AnalysisResponse(AnalysisCreate):
    id: int
    created_at: datetime
    updated_at: datetime
    class Config:
        from_attributes = True

# Calculated fields
class CalculatedFieldBase(BaseModel):
    analysis_id: int
    field_name: str
    formula: str
    default_agg: Optional[str] = None

class CalculatedFieldCreate(CalculatedFieldBase):
    pass

class CalculatedFieldOut(CalculatedFieldBase):
    id: int
    class Config:
        from_attributes = True

# Values config for pivot
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

# Filters
class FilterSaveRequest(BaseModel):
    dataset_id: int
    analysis_id: int
    # selected_columns can be either list[str] or dict[str, List[Any]]
    selected_columns: Any

class FilterResponse(BaseModel):
    id: int
    dataset_id: int
    analysis_id: int
    selected_columns: Any
    class Config:
        from_attributes = True

# Reports & Sheets
class ReportCreate(BaseModel):
    name: str

class ReportResponse(ReportCreate):
    id: int
    created_at: datetime
    class Config:
        from_attributes = True

class ReportRename(BaseModel):
    name: str

class SheetCreate(BaseModel):
    name: str
    report_id: int

class SheetResponse(BaseModel):
    id: int
    name: str
    report_id: int
    class Config:
        from_attributes = True

class SheetAnalysisMapIn(BaseModel):
    analysis_id: int

class SheetAnalysisMapOut(BaseModel):
    id: int
    sheet_id: int
    analysis_id: int
    class Config:
        from_attributes = True

class SheetDetailResponse(BaseModel):
    sheet_id: int
    name: str
    report_id: int
    analyses: List[Dict[str, Any]]
