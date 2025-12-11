# crud.py
import boto3
import pandas as pd
from io import BytesIO
from sqlalchemy.orm import Session
from typing import List, Any, Optional
import os
from dotenv import load_dotenv
from models import (DatasetMetadata, Analysis, CalculatedField, FilterSelection,
                    Report, Sheet, SheetAnalysisMap)

load_dotenv()

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# S3 helpers
def get_latest_file_from_s3(bucket: str, prefix: str) -> str:
    resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in resp:
        raise Exception("No files found in S3 prefix")
    latest_obj = max(resp["Contents"], key=lambda x: x["LastModified"])
    return latest_obj["Key"]

def fetch_dataset_from_s3(bucket: str, key: str) -> pd.DataFrame:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    raw = obj["Body"].read()
    if key.endswith(".parquet"):
        return pd.read_parquet(BytesIO(raw))
    elif key.endswith(".csv"):
        return pd.read_csv(BytesIO(raw))
    elif key.endswith((".xlsx", ".xls")):
        return pd.read_excel(BytesIO(raw))
    else:
        raise Exception(f"Unsupported file type: {key}")

# Dataset metadata
def create_dataset_metadata(db: Session, data, latest_file: str):
    db_item = DatasetMetadata(**data.dict(), latest_file=latest_file)
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

def get_all_datasets(db: Session):
    return db.query(DatasetMetadata).all()

def get_dataset_by_id(db: Session, dataset_id: int) -> Optional[DatasetMetadata]:
    return db.query(DatasetMetadata).filter(DatasetMetadata.id == dataset_id).first()

# Analysis
def create_analysis(db: Session, analysis):
    db_analysis = Analysis(
        dataset_id=analysis.dataset_id,
        analysis_name=analysis.analysis_name,
        analysis_type=analysis.analysis_type,
        config=analysis.config
    )
    db.add(db_analysis)
    db.commit()
    db.refresh(db_analysis)
    return db_analysis

def get_analyses_by_dataset(db: Session, dataset_id: int):
    return db.query(Analysis).filter(Analysis.dataset_id == dataset_id).all()

def get_analysis(db: Session, analysis_id: int):
    return db.query(Analysis).filter(Analysis.id == analysis_id).first()

def update_analysis_config(db: Session, analysis_id: int, config: dict):
    analysis = get_analysis(db, analysis_id)
    if analysis:
        analysis.config = config
        db.commit()
        db.refresh(analysis)
    return analysis

# Calculated fields
def create_calculated_field(db: Session, payload):
    analysis = get_analysis(db, payload.analysis_id)
    if not analysis:
        raise Exception("Analysis not found")
    calc = CalculatedField(
        analysis_id=payload.analysis_id,
        dataset_id=analysis.dataset_id,
        field_name=payload.field_name,
        formula=payload.formula,
        default_agg=payload.default_agg
    )
    db.add(calc)
    db.commit()
    db.refresh(calc)
    return calc

def get_calculated_fields_by_analysis(db: Session, analysis_id: int) -> List[CalculatedField]:
    return db.query(CalculatedField).filter(CalculatedField.analysis_id == analysis_id).all()

def delete_calculated_field(db: Session, field_id: int) -> bool:
    obj = db.query(CalculatedField).filter(CalculatedField.id == field_id).first()
    if not obj:
        return False
    db.delete(obj)
    db.commit()
    return True

# Filters
def save_filter(db: Session, dataset_id: int, analysis_id: int, selected_columns: Any):
    existing = db.query(FilterSelection).filter_by(dataset_id=int(dataset_id), analysis_id=int(analysis_id)).first()
    if existing:
        existing.selected_columns = selected_columns
        db.add(existing)
        db.commit()
        db.refresh(existing)
        return existing
    new_entry = FilterSelection(
        dataset_id=int(dataset_id),
        analysis_id=int(analysis_id),
        selected_columns=selected_columns
    )
    db.add(new_entry)
    db.commit()
    db.refresh(new_entry)
    return new_entry

def get_saved_filter(db: Session, dataset_id: int, analysis_id: int) -> Any:
    rec = db.query(FilterSelection).filter_by(dataset_id=int(dataset_id), analysis_id=int(analysis_id)).first()
    return rec.selected_columns if rec else None

def delete_filter(db: Session, dataset_id: int, analysis_id: int) -> bool:
    rec = db.query(FilterSelection).filter_by(dataset_id=int(dataset_id), analysis_id=int(analysis_id)).first()
    if not rec:
        return False
    db.delete(rec)
    db.commit()
    return True

# Reports & Sheets
def create_report(db: Session, name: str):
    r = Report(name=name)
    db.add(r)
    db.commit()
    db.refresh(r)
    return r

def create_sheet(db: Session, name: str, report_id: int):
    s = Sheet(name=name, report_id=report_id)
    db.add(s)
    db.commit()
    db.refresh(s)
    return s

def add_analysis_to_sheet(db: Session, sheet_id: int, analysis_id: int):
    mapping = SheetAnalysisMap(sheet_id=sheet_id, analysis_id=analysis_id)
    db.add(mapping)
    db.commit()
    db.refresh(mapping)
    return mapping

def get_all_reports(db: Session):
    return db.query(Report).all()
def get_sheet(db: Session, sheet_id: int):
    return db.query(Sheet).filter(Sheet.id == sheet_id).first()

def get_report(db: Session, report_id: int):
    return db.query(Report).filter(Report.id == report_id).first()


# Delete Report
def delete_report(db: Session, report_id: int):
    report = db.query(Report).filter(Report.id == report_id).first()
    if not report:
        return False
    
    # Delete related sheets, mappings & analyses mapped in sheets
    for sheet in report.sheets:
        db.query(SheetAnalysisMap).filter_by(sheet_id=sheet.id).delete()
        db.query(Sheet).filter(Sheet.id == sheet.id).delete()

    db.delete(report)
    db.commit()
    return True


# Delete Sheet
def delete_sheet(db: Session, sheet_id: int):
    sheet = db.query(Sheet).filter(Sheet.id == sheet_id).first()
    if not sheet:
        return False
    
    # Delete mappings under sheet
    db.query(SheetAnalysisMap).filter_by(sheet_id=sheet_id).delete()

    db.delete(sheet)
    db.commit()
    return True
