from sqlalchemy import Column, Integer, String, DateTime , ForeignKey, JSON
from db import Base
from datetime import datetime
from sqlalchemy.sql import func

class DatasetMetadata(Base):
    __tablename__ = "dataset_metadata"

    id = Column(Integer, primary_key=True, index=True)
    dataset_name = Column(String, index=True)  
    s3_bucket = Column(String, nullable=False)
    s3_key = Column(String, nullable=False)
    latest_file = Column(String, nullable=False)
    num_rows = Column(Integer)
    num_columns = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Analysis(Base):
    __tablename__ = "analyses"

    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("dataset_metadata.id"), nullable=False)
    analysis_name = Column(String, nullable=False)
    analysis_type = Column(String, nullable=False)
    config = Column(JSON, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
