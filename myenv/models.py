from sqlalchemy import Column, Integer, String, DateTime
from db import Base
from datetime import datetime

class DatasetMetadata(Base):
    __tablename__ = "dataset_metadata"

    id = Column(Integer, primary_key=True, index=True)
    dataset_name = Column(String, index=True)  # Human-readable dataset name
    s3_bucket = Column(String, nullable=False)
    s3_key = Column(String, nullable=False)
    num_rows = Column(Integer)
    num_columns = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
