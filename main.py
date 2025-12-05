from fastapi import FastAPI, HTTPException, Depends , Query
from sqlalchemy.orm import Session
import crud, schemas
from db import get_db ,Base, engine
import boto3 , io
import numpy as np
from datetime import datetime
from typing import List , Optional
import pandas as pd 
from fastapi import Body
import re
import models

# Create tables
Base.metadata.create_all(bind=engine)

app = FastAPI()

def apply_formula(df: pd.DataFrame, formula: str):
    expr = formula

    for col in df.columns:
        expr = re.sub(rf'\b{col}\b', f'df["{col}"]', expr)

    expr = re.sub(
        r'ifelse\s*\((.*?),(.*?),(.*)\)',
        r'np.where(\1, \2, \3)',
        expr
    )

    try:
        result = eval(expr, {"df": df, "np": np})
    except Exception as e:
        raise ValueError(f"Invalid formula: {formula} | Error: {str(e)}")

    return result

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
@app.get("/analysis/{analysis_id}/columns")
def get_all_columns_for_analysis(analysis_id: int, db: Session = Depends(get_db)):
    analysis = crud.get_analysis(db, analysis_id)
    if not analysis:
        raise HTTPException(404, "Analysis not found")

    dataset_id = analysis.dataset_id

    # 1️⃣ Get dataset columns
    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Dataset not found")

    bucket = metadata.s3_bucket
    prefix = metadata.s3_key

    latest_file = crud.get_latest_file_from_s3(bucket, prefix)
    if not latest_file:
        raise HTTPException(404, "No files found in S3 folder")

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=latest_file)
    raw = obj["Body"].read()

    # Read header only
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
        raise HTTPException(400, "Unsupported file type")

    dataset_columns = list(df.columns)

    # 2️⃣ Get calculated fields for this analysis
    calc_fields = crud.get_calculated_fields_by_analysis(db, analysis_id)
    calc_field_names = [f.field_name  for f in calc_fields]

    return {
        "columns": dataset_columns + calc_field_names
    }




@app.get("/datasets/{dataset_id}/analyses", response_model=List[schemas.AnalysisResponse])
def get_dataset_analyses(dataset_id: int, db: Session = Depends(get_db)):
    return crud.get_analyses_by_dataset(db, dataset_id)

@app.post("/analyses/", response_model=schemas.AnalysisResponse)
def create_analysis(analysis: schemas.AnalysisCreate, db: Session = Depends(get_db)):
    return crud.create_analysis(db, analysis)



@app.post("/analysis/preview")
def analysis_preview(payload: schemas.AnalysisPreviewRequest, db: Session = Depends(get_db)):

    dataset_id = payload.dataset_id
    analysis_id = payload.analysis_id
    analysis_type = payload.type.lower()

    rows = payload.rows or []
    columns = payload.columns or []
    values_config = payload.values or []

    if not dataset_id:
        raise HTTPException(400, "dataset_id is required")
    if not analysis_id:
        raise HTTPException(400, "analysis_id is required")

    # ---------------- Get Dataset ----------------
    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(404, "Dataset not found")

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=metadata.s3_bucket, Key=metadata.latest_file)
    raw = obj["Body"].read()

    # -------- Read file --------
    if metadata.latest_file.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw))
    elif metadata.latest_file.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(raw))
    elif metadata.latest_file.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(raw))
    else:
        raise HTTPException(400, "Unsupported file type")

    # ================= APPLY CALCULATED FIELDS ================
    calc_fields = crud.get_calculated_fields_by_analysis(db, analysis_id)

    for field in calc_fields:
        try:
            df[field.field_name] = apply_formula(df, field.formula)
            df[field.field_name] = pd.to_numeric(df[field.field_name], errors='ignore')
        except Exception as e:
            raise HTTPException(400, f"Formula Error in {field.field_name}: {e}")

    # ---------------- APPLY SAVED FILTERS ----------------
    saved_filters = crud.get_saved_filter(db, dataset_id, analysis_id)  # returns a list of columns
    filtered_columns = []

    if saved_filters:
        for col in saved_filters:
            if col in df.columns:
                # If you only have column selection (no specific values), just keep the column
                filtered_columns.append(col)

        # Keep only selected columns + any columns needed for pivot
        df = df[rows + columns + filtered_columns + [c for c in df.columns if c not in rows + columns + filtered_columns]]

    # ===========================================================
    #                          PIVOT
    # ===========================================================
    if analysis_type == "pivot":
        if not values_config:
            values_config = []  # initialize list if user didn't send values

        # first include values selected by user
        agg_dict = {v.column: v.agg for v in values_config}
        value_cols = [v.column for v in values_config]

        # add calculated fields automatically
        for field in calc_fields:
            agg_dict[field.field_name] = field.default_agg
            value_cols.append(field.field_name)

        # --- Generate Pivot ---
        try:
            pivot = pd.pivot_table(
                df,
                index=rows,
                columns=columns,
                values=value_cols if value_cols else None,
                aggfunc=agg_dict if agg_dict else "sum",
                margins=True,
                margins_name="Total"
            )
        except Exception as e:
            raise HTTPException(400, f"Pivot Error: {e}")

        pivot = pivot.reset_index()

        # Flatten MultiIndex
        pivot.columns = [
            "_".join([str(x) for x in col if x not in ["", None]])
            if isinstance(col, tuple) else str(col)
            for col in pivot.columns
        ]

        # Keep Total only at bottom
        total_row = pivot[pivot.apply(lambda r: "Total" in " ".join(r.astype(str)), axis=1)]
        pivot = pivot[~pivot.apply(lambda r: "Total" in " ".join(r.astype(str)), axis=1)]
        pivot = pd.concat([pivot, total_row], ignore_index=True)

        return {
            "columns": pivot.columns.tolist(),
            "count": len(pivot),
            "table": pivot.to_dict(orient="records"),
            "calculated_fields_used": [f.field_name for f in calc_fields],
            "filtered_columns": filtered_columns
        }

    return {"message": "Other analysis types coming soon"}







# CREATE
@app.post("/calculated-fields", response_model= schemas.CalculatedFieldOut)
def create_calc_field(payload: schemas.CalculatedFieldCreate, db: Session = Depends(get_db)):
    return crud.create_calculated_field(db, payload)

# LIST
@app.get("/analysis/{analysis_id}/calculated-fields", response_model=list[schemas.CalculatedFieldOut])
def list_calc_fields(analysis_id: int, db: Session = Depends(get_db)):
    return crud.get_calculated_fields_by_analysis(db, analysis_id)

# DELETE
@app.delete("/calculated-field/{field_id}")
def delete_calc_field(field_id: int, db: Session = Depends(get_db)):
    ok = crud.delete_calculated_field(db, field_id)
    if not ok:
        raise HTTPException(404, "Calculated field not found")
    return {"status": "deleted"}


# ---------------- Save filter selection ----------------
@app.post("/filters/save", response_model=schemas.FilterResponse)
def save_filter(req: schemas.FilterSaveRequest, db: Session = Depends(get_db)):
    """
    Save selected columns for a specific dataset + analysis.
    If a filter already exists for the analysis_id, update it.
    """
    # Validate analysis exists
    analysis = crud.get_analysis(db, req.analysis_id)
    if not analysis:
        raise HTTPException(404, detail="Analysis not found")

    # Validate dataset matches analysis
    if str(analysis.dataset_id) != str(req.dataset_id):
        raise HTTPException(400, detail="Dataset ID does not match the analysis")

    # Check if filter already exists
    existing_filter = db.query(models.FilterSelection).filter(
        models.FilterSelection.analysis_id == req.analysis_id
    ).first()

    if existing_filter:
        # Update existing filter
        existing_filter.selected_columns = req.selected_columns
        existing_filter.updated_at = datetime.utcnow()
        db.add(existing_filter)
        db.commit()
        db.refresh(existing_filter)
        return existing_filter
    else:
        # Create new filter
        new_filter = models.FilterSelection(
            dataset_id=req.dataset_id,
            analysis_id=req.analysis_id,
            selected_columns=req.selected_columns
        )
        db.add(new_filter)
        db.commit()
        db.refresh(new_filter)
        return new_filter


# ---------------- Get saved filters ----------------
@app.get("/filters/saved", response_model=schemas.FilterResponse)
def get_saved_filter(dataset_id: str, analysis_id: int, db: Session = Depends(get_db)):
    """
    Get saved filter selection for a dataset + analysis.
    """
    filter_obj = db.query(models.FilterSelection).filter(
        models.FilterSelection.dataset_id == dataset_id,
        models.FilterSelection.analysis_id == analysis_id
    ).first()

    if not filter_obj:
        raise HTTPException(404, detail="No saved filter found")

    return filter_obj


@app.delete("/filters")
def delete_filter(dataset_id: str, analysis_id: int, db: Session = Depends(get_db)):
    """
    Delete saved filters for a specific dataset and analysis.
    """
    # Check if filter exists
    filter_record = db.query(crud.FilterSelection).filter_by(
        dataset_id=dataset_id,
        analysis_id=analysis_id
    ).first()

    if not filter_record:
        raise HTTPException(status_code=404, detail="Filter not found")

    db.delete(filter_record)
    db.commit()

    return {"message": f"Filters for dataset_id={dataset_id} and analysis_id={analysis_id} deleted successfully"}

