from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class DatasetMetadataCreate(BaseModel):
    dataset_name: str
    s3_bucket: str
    s3_key: str
    num_rows: Optional[int] = None   # will be computed
    num_columns: Optional[int] = None

class DatasetMetadataResponse(DatasetMetadataCreate):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
