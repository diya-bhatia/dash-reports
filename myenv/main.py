from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
import crud, schemas
from db import get_db ,Base, engine
import boto3 , io
import numpy as np
from datetime import datetime
from typing import List
import pandas as pd 
# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI()

# ---------------- Upload dataset ----------------
@app.post("/datasets/", response_model=schemas.DatasetMetadataResponse)
def upload_dataset(dataset: schemas.DatasetMetadataCreate, db: Session = Depends(get_db)):
    try:
        # Get the latest file in the given S3 prefix
        latest_file = crud.get_latest_file_from_s3(dataset.s3_bucket, dataset.s3_key)
        # Fetch dataframe to compute metadata
        df = crud.fetch_dataset_from_s3(dataset.s3_bucket, latest_file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"S3 Error: {str(e)}")

    dataset.num_rows = len(df)
    dataset.num_columns = len(df.columns)

    # Save metadata + latest_file
    return crud.create_dataset_metadata(db, dataset, latest_file)

# ---------------- List datasets ----------------
@app.get("/datasets/", response_model=list[schemas.DatasetMetadataResponse])
def list_datasets(db: Session = Depends(get_db)):
    return crud.get_all_datasets(db)

# ---------------- Get paginated data ----------------
@app.get("/datasets/{dataset_id}/data")
def get_dataset_data(
    dataset_id: int,
    db: Session = Depends(get_db),
    page: int = 1,
    limit: int = 500
):
    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Dataset not found")

    bucket = metadata.s3_bucket
    prefix = metadata.s3_key

    try:
        # Fetch latest file from the prefix
        latest_file = crud.get_latest_file_from_s3(bucket, prefix)
        if not latest_file:
            raise HTTPException(status_code=404, detail="No files found in S3 prefix")

        # Update metadata if latest_file has changed
        if metadata.latest_file != latest_file:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket, Key=latest_file)
            raw = obj["Body"].read()

            # Determine file type to read
            if latest_file.endswith(".csv"):
                try:
                    df_latest = pd.read_csv(io.BytesIO(raw), encoding="utf-8")
                except UnicodeDecodeError:
                    df_latest = pd.read_csv(io.BytesIO(raw), encoding="latin1")
            elif latest_file.endswith((".xlsx", ".xls")):
                df_latest = pd.read_excel(io.BytesIO(raw))
            elif latest_file.endswith(".parquet"):
                df_latest = pd.read_parquet(io.BytesIO(raw))
            else:
                raise HTTPException(status_code=400, detail=f"Unsupported file type for {latest_file}")

            # Update metadata
            metadata.latest_file = latest_file
            metadata.num_rows = len(df_latest)
            metadata.num_columns = len(df_latest.columns)
            metadata.updated_at = datetime.utcnow()
            db.commit()

        # Fetch the dataset for the current page
        s3 = boto3.client("s3")
        obj = s3.get_object(Bucket=bucket, Key=latest_file)
        raw = obj["Body"].read()

        if latest_file.endswith(".csv"):
            try:
                df = pd.read_csv(io.BytesIO(raw), encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(io.BytesIO(raw), encoding="latin1")
        elif latest_file.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(raw))
        elif latest_file.endswith(".parquet"):
            df = pd.read_parquet(io.BytesIO(raw))
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type for {latest_file}")

        # Pagination
        total_rows = len(df)
        start = (page - 1) * limit
        end = start + limit
        df_page = df.iloc[start:end].replace({np.nan: None})

        return {
            "dataset_name": metadata.dataset_name,
            "latest_file": latest_file,
            "page": page,
            "limit": limit,
            "total_rows": total_rows,
            "total_pages": (total_rows + limit - 1) // limit,
            "data": df_page.to_dict(orient="records")
        }

    except boto3.exceptions.Boto3Error as e:
        raise HTTPException(status_code=500, detail=f"S3 Error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


# ---------------- Get dataset columns ----------------
@app.get("/datasets/{dataset_id}/columns")
def get_dataset_columns(dataset_id: int, db: Session = Depends(get_db)):
    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Dataset not found")

    bucket = metadata.s3_bucket
    prefix = metadata.s3_key

    # Get latest file
    latest_file = crud.get_latest_file_from_s3(bucket, prefix)
    if not latest_file:
        raise HTTPException(status_code=404, detail="No files in S3 prefix")

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=latest_file)
    raw = obj["Body"].read()

    # Read ONLY header
    if latest_file.endswith(".csv"):
        try:
            df = pd.read_csv(io.BytesIO(raw), nrows=0, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(raw), nrows=0, encoding="latin1")

    elif latest_file.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(raw), nrows=0)

    elif latest_file.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(raw)).head(0)

    else:
        raise HTTPException(status_code=400, detail="Unsupported file type")

    return {"columns": list(df.columns)}



@app.get("/datasets/{dataset_id}/analyses", response_model=List[schemas.AnalysisResponse])
def get_dataset_analyses(dataset_id: int, db: Session = Depends(get_db)):
    return crud.get_analyses_by_dataset(db, dataset_id)

@app.post("/analyses/", response_model=schemas.AnalysisResponse)
def create_analysis(analysis: schemas.AnalysisCreate, db: Session = Depends(get_db)):
    return crud.create_analysis(db, analysis)

@app.post("/analysis/preview")
def analysis_preview(payload: dict, db: Session = Depends(get_db)):

    dataset_id = payload.get("dataset_id")
    analysis_type = payload.get("analysis_type")
    columns = payload.get("columns", [])
    aggregations = payload.get("aggregations", {})

    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(404, "Dataset not found")

    # load dataset from S3
    bucket = metadata.s3_bucket
    latest_file = metadata.latest_file

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=latest_file)
    raw = obj["Body"].read()

    if latest_file.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw))
    elif latest_file.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(raw))
    elif latest_file.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(raw))
    else:
        raise HTTPException(400, "Unsupported file format")

    # generate preview
    if analysis_type == "pivot":
        pivot = df.pivot_table(
            index=columns.get("rows", []),
            columns=columns.get("columns", []),
            values=columns.get("values", []),
            aggfunc="sum"
        ).reset_index()

        return {"preview": pivot.to_dict(orient="records")}

    if analysis_type == "bar":
        x = columns.get("x")
        y = columns.get("y")

        bar_df = df[[x, y]].groupby(x).sum().reset_index()

        return {"preview": bar_df.to_dict(orient="records")}

    return {"preview": []}
