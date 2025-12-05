from http.client import HTTPException
import boto3
import pandas as pd
from io import BytesIO
from sqlalchemy.orm import Session
from models import DatasetMetadata , Analysis ,CalculatedField , FilterSelection
from schemas import DatasetMetadataCreate , AnalysisCreate ,CalculatedFieldCreate
import os
from dotenv import load_dotenv
from typing import List

load_dotenv()

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

# ---------------- Get latest file in a prefix ----------------
def get_latest_file_from_s3(bucket: str, prefix: str) -> str:
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in resp:
        raise Exception("No files found in S3 prefix")

    latest_obj = max(resp["Contents"], key=lambda x: x["LastModified"])
    return latest_obj["Key"]

# ---------------- Fetch dataset ----------------
def fetch_dataset_from_s3(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()

    if key.endswith(".parquet"):
        df = pd.read_parquet(BytesIO(data))
    elif key.endswith(".csv"):
        df = pd.read_csv(BytesIO(data))
    elif key.endswith(".xlsx") or key.endswith(".xls"):
        df = pd.read_excel(BytesIO(data))
    else:
        raise Exception(f"Unsupported file type: {key}")

    return df

# ---------------- CRUD DB operations ----------------
def create_dataset_metadata(db: Session, data: DatasetMetadataCreate, latest_file: str):
    db_item = DatasetMetadata(**data.dict(), latest_file=latest_file)
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

def get_all_datasets(db: Session):
    return db.query(DatasetMetadata).all()

def get_dataset_by_id(db: Session, dataset_id: int):
    return db.query(DatasetMetadata).filter(DatasetMetadata.id == dataset_id).first()



def create_analysis(db: Session, analysis: AnalysisCreate):
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
    analysis = db.query(Analysis).filter(Analysis.id == analysis_id).first()
    if analysis:
        analysis.config = config
        db.commit()
        db.refresh(analysis)
    return analysis


def create_calculated_field(db: Session, payload: CalculatedFieldCreate):
    # Fetch analysis → get dataset id
    analysis = db.query(Analysis).filter(Analysis.id == payload.analysis_id).first()
    if not analysis:
        raise HTTPException(404, "Analysis not found")

    calc = CalculatedField(
        analysis_id = payload.analysis_id,
        dataset_id = analysis.dataset_id,  # auto filling
        field_name = payload.field_name,
        formula = payload.formula,
        default_agg = payload.default_agg
    )

    db.add(calc)
    db.commit()
    db.refresh(calc)
    return calc


def get_calculated_fields_by_analysis(db: Session, analysis_id: int):
    return db.query(CalculatedField).filter(
        CalculatedField.analysis_id == analysis_id
    ).all()


def delete_calculated_field(db: Session, field_id: int):
    obj = db.query(CalculatedField).filter(CalculatedField.id == field_id).first()
    if not obj:
        return False
    db.delete(obj)
    db.commit()
    return True


def save_filter(db: Session, dataset_id: str, analysis_id: int, selected_columns: List[str]):
    # Check if record already exists
    existing = db.query(FilterSelection).filter_by(
        dataset_id=str(dataset_id),
        analysis_id=int(analysis_id)
    ).first()

    if existing:
        existing.selected_columns = selected_columns
    else:
        new_entry = FilterSelection(
            dataset_id=dataset_id,
            analysis_id=analysis_id,
            selected_columns=selected_columns
        )
        db.add(new_entry)

    db.commit()
    return {"message": "Filters saved successfully"}


def get_saved_filter(db: Session, dataset_id: str, analysis_id: int):
    record = db.query(FilterSelection).filter_by(
        dataset_id=str(dataset_id),
        analysis_id=int(analysis_id)
    ).first()

    return record.selected_columns if record else []