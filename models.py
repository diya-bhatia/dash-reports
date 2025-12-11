# models.py
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, JSON, func
from sqlalchemy.orm import relationship
from db import Base
from datetime import datetime

# Dataset metadata
class DatasetMetadata(Base):
    __tablename__ = "dataset_metadata"
    id = Column(Integer, primary_key=True, index=True)
    dataset_name = Column(String, index=True)
    s3_bucket = Column(String, nullable=False)
    s3_key = Column(String, nullable=False)
    latest_file = Column(String, nullable=True)
    num_rows = Column(Integer)
    num_columns = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    analyses = relationship("Analysis", back_populates="dataset", cascade="all, delete")


# Reports (top-level container)
class Report(Base):
    __tablename__ = "reports"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, unique=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    sheets = relationship("Sheet", back_populates="report", cascade="all, delete")


# Sheets (group of analyses inside a report)
class Sheet(Base):
    __tablename__ = "sheets"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False, index=True)
    report_id = Column(Integer, ForeignKey("reports.id"), nullable=False)

    report = relationship("Report", back_populates="sheets")
    sheet_maps = relationship("SheetAnalysisMap", back_populates="sheet", cascade="all, delete")


# mapping table sheet <-> analysis (many-to-many via mapping)
class SheetAnalysisMap(Base):
    __tablename__ = "sheet_analysis_map"
    id = Column(Integer, primary_key=True, index=True)
    sheet_id = Column(Integer, ForeignKey("sheets.id"), nullable=False)
    analysis_id = Column(Integer, ForeignKey("analyses.id"), nullable=False)

    sheet = relationship("Sheet", back_populates="sheet_maps")
    analysis = relationship("Analysis", back_populates="sheet_links")


# Analyses (one dataset per analysis)
class Analysis(Base):
    __tablename__ = "analyses"
    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, ForeignKey("dataset_metadata.id"), nullable=False)
    analysis_name = Column(String, nullable=False)
    analysis_type = Column(String, nullable=False)  # e.g., "pivot", "bar"
    config = Column(JSON, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    dataset = relationship("DatasetMetadata", back_populates="analyses")
    calculated_fields = relationship("CalculatedField", back_populates="analysis", cascade="all, delete")
    filters = relationship("FilterSelection", back_populates="analysis", cascade="all, delete")
    sheet_links = relationship("SheetAnalysisMap", back_populates="analysis")


# Calculated fields per analysis
class CalculatedField(Base):
    __tablename__ = "calculated_fields"
    id = Column(Integer, primary_key=True, index=True)
    analysis_id = Column(Integer, ForeignKey("analyses.id"), nullable=False)
    dataset_id = Column(Integer, ForeignKey("dataset_metadata.id"), nullable=False)
    field_name = Column(String, nullable=False)
    formula = Column(String, nullable=False)
    default_agg = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    analysis = relationship("Analysis", back_populates="calculated_fields")


# Filters: selected_columns can be either a list of column names (["col1","col2"])
# or a dict mapping column->list-of-values ({"col1":["a","b"], "col2":[..]})
class FilterSelection(Base):
    __tablename__ = "filters"
    id = Column(Integer, primary_key=True, index=True)
    analysis_id = Column(Integer, ForeignKey("analyses.id"), nullable=False)
    dataset_id = Column(Integer, ForeignKey("dataset_metadata.id"), nullable=False)
    selected_columns = Column(JSON, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    analysis = relationship("Analysis", back_populates="filters")
