# main.py
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
import models, schemas, crud
from db import Base, engine, get_db
import pandas as pd
import boto3
import io
import numpy as np


# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI(title="S3 Dataset Metadata API")

@app.post("/datasets/", response_model=schemas.DatasetMetadataResponse)
def upload_dataset(
    dataset: schemas.DatasetMetadataCreate,  # only dataset_name, s3_bucket, s3_key required
    db: Session = Depends(get_db)
):
    """
    Fetch Parquet dataset from S3, compute metadata, store in Postgres.
    """
    try:
        df = crud.fetch_dataset_from_s3(dataset.s3_bucket, dataset.s3_key)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"S3 Error: {str(e)}")

    # Compute rows and columns
    dataset.num_rows = len(df)
    dataset.num_columns = len(df.columns)

    # Store in DB
    return crud.create_dataset_metadata(db, dataset)


@app.get("/datasets/", response_model=list[schemas.DatasetMetadataResponse])
def list_datasets(db: Session = Depends(get_db)):
    """
    Get all dataset metadata.
    """
    return crud.get_all_datasets(db)


@app.get("/datasets/{dataset_id}/data")
def get_dataset_data(
    dataset_id: int,
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 500  # how many rows per page
):
    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Dataset not found")

    bucket = metadata.s3_bucket
    key = metadata.s3_key

    try:
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=key)
        raw = obj["Body"].read()

        # ---- Determine file type ----
        if key.endswith(".csv"):
            try:
                df = pd.read_csv(io.BytesIO(raw), encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(raw), encoding="latin1")

        elif key.endswith(".xlsx") or key.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(raw))

        elif key.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(raw))

        else:
            raise HTTPException(status_code=400,
                                detail=f"Unsupported file type for {key}")

        # ---- Pagination logic ----
        total_rows = len(df)
        start = (page - 1) * limit
        end = start + limit

        df_page = df.iloc[start:end].replace({np.nan: None})

        return {
            "dataset_name": metadata.dataset_name,
            "page": page,
            "limit": limit,
            "total_rows": total_rows,
            "total_pages": (total_rows + limit - 1) // limit,
            "data": df_page.to_dict(orient="records")
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))