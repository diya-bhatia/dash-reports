from sqlalchemy.orm import Session
from models import DatasetMetadata
from schemas import DatasetMetadataCreate
import boto3
import pandas as pd
from io import BytesIO
import os
from dotenv import load_dotenv
import models

load_dotenv()

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION")
)

def fetch_dataset_from_s3(bucket: str, key: str):
    """
    Fetch a Parquet dataset from S3 and return a pandas DataFrame.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(BytesIO(obj['Body'].read()))
    return df

def create_dataset_metadata(db: Session, data: DatasetMetadataCreate):
    """
    Store dataset metadata in Postgres.
    """
    db_item = DatasetMetadata(**data.dict())
    db.add(db_item)
    db.commit()
    db.refresh(db_item)
    return db_item

def get_all_datasets(db: Session):
    """
    Retrieve all datasets from Postgres.
    """
    return db.query(DatasetMetadata).all()

def fetch_dataset_from_s3(bucket: str, key: str):
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()

    # Parquet
    if key.endswith(".parquet"):
        df = pd.read_parquet(BytesIO(data))

    # CSV
    elif key.endswith(".csv"):
        df = pd.read_csv(BytesIO(data))

    return df


# ---------- Fetch dataset by ID ----------
def get_dataset_by_id(db: Session, dataset_id: int):
    return db.query(models.DatasetMetadata).filter(models.DatasetMetadata.id == dataset_id).first()

