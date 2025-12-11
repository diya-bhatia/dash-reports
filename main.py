# main.py
from fastapi import FastAPI, HTTPException, Depends, Query
from sqlalchemy.orm import Session
import crud, schemas, models
from db import get_db, Base, engine
import boto3, io
import numpy as np
from datetime import datetime
from typing import List, Optional, Any
import pandas as pd
import re

# create DB tables (run once; if you change models use migrations)
Base.metadata.create_all(bind=engine)

app = FastAPI(title="Pivot/Sheets/Reports API")

def apply_formula(df: pd.DataFrame, formula: str):
    expr = formula
    # Replace column names with df["col"]
    for col in df.columns:
        expr = re.sub(rf'\b{re.escape(col)}\b', f'df["{col}"]', expr)
    # simple ifelse -> np.where conversion
    expr = re.sub(r'ifelse\s*\((.*?),(.*?),(.*)\)', r'np.where(\1, \2, \3)', expr)
    try:
        result = eval(expr, {"df": df, "np": np, "pd": pd})
    except Exception as e:
        raise ValueError(f"Invalid formula: {formula} | Error: {e}")
    return result

# ---------------- Reports & Sheets ----------------
@app.post("/reports/", response_model=schemas.ReportResponse)
def create_report(req: schemas.ReportCreate, db: Session = Depends(get_db)):
    existing = crud.get_report(db, None) if False else None  # placeholder
    report = crud.create_report(db, req.name)
    return report

@app.get("/reports/")
def list_reports(db: Session = Depends(get_db)):
    reps = crud.get_all_reports(db)
    return [{"id": r.id, "name": r.name} for r in reps]

@app.post("/reports/{report_id}/sheets", response_model=schemas.SheetResponse)
def create_sheet(report_id: int, req: schemas.SheetCreate, db: Session = Depends(get_db)):
    # validate report exists
    rep = crud.get_report(db, report_id)
    if not rep:
        raise HTTPException(404, "Report not found")
    sheet = crud.create_sheet(db, req.name, report_id)
    return sheet

@app.post("/sheets/{sheet_id}/add-analysis", response_model=schemas.SheetAnalysisMapOut)
def add_analysis_to_sheet(sheet_id: int, req: schemas.SheetAnalysisMapIn, db: Session = Depends(get_db)):
    sheet = crud.get_sheet(db, sheet_id)
    if not sheet:
        raise HTTPException(404, "Sheet not found")
    analysis = crud.get_analysis(db, req.analysis_id)
    if not analysis:
        raise HTTPException(404, "Analysis not found")
    mapping = crud.add_analysis_to_sheet(db, sheet_id, req.analysis_id)
    return mapping

@app.get("/reports/{report_id}")
def get_report(report_id: int, db: Session = Depends(get_db)):
    rep = crud.get_report(db, report_id)
    if not rep:
        raise HTTPException(404, "Report not found")
    sheets = []
    for s in rep.sheets:
        s_maps = s.sheet_maps
        analyses = []
        for m in s_maps:
            a = crud.get_analysis(db, m.analysis_id)
            ds = crud.get_dataset_by_id(db, a.dataset_id)
            analyses.append({
                "analysis_id": a.id,
                "analysis_name": a.analysis_name,
                "dataset_id": ds.id,
                "dataset_name": ds.dataset_name
            })
        sheets.append({"sheet_id": s.id, "name": s.name, "analyses": analyses})
    return {"report_id": rep.id, "name": rep.name, "sheets": sheets}

# ---------------- Dataset endpoints ----------------
@app.post("/datasets/", response_model=schemas.DatasetMetadataResponse)
def upload_dataset(dataset: schemas.DatasetMetadataCreate, db: Session = Depends(get_db)):
    try:
        latest_file = crud.get_latest_file_from_s3(dataset.s3_bucket, dataset.s3_key)
        df = crud.fetch_dataset_from_s3(dataset.s3_bucket, latest_file)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"S3 Error: {e}")
    dataset.num_rows = len(df)
    dataset.num_columns = len(df.columns)
    return crud.create_dataset_metadata(db, dataset, latest_file)

@app.get("/datasets/", response_model=List[schemas.DatasetMetadataResponse])
def list_datasets(db: Session = Depends(get_db)):
    return crud.get_all_datasets(db)

@app.get("/datasets/{dataset_id}/data")
def get_dataset_data(dataset_id: int, db: Session = Depends(get_db), page: int = 1, limit: int = 500):
    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(status_code=404, detail="Dataset not found")
    bucket = metadata.s3_bucket
    prefix = metadata.s3_key
    try:
        latest_file = crud.get_latest_file_from_s3(bucket, prefix)
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=latest_file)
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
            raise HTTPException(400, detail="Unsupported file type")
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------------- Get dataset columns ----------------
@app.get("/datasets/{dataset_id}/columns")
def get_dataset_columns(dataset_id: int, db: Session = Depends(get_db)):
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
    return {"columns": dataset_columns}

# ---------------- Analysis endpoints ----------------
@app.post("/analyses/", response_model=schemas.AnalysisResponse)
def create_analysis(analysis: schemas.AnalysisCreate, db: Session = Depends(get_db)):
    ds = crud.get_dataset_by_id(db, analysis.dataset_id)
    if not ds:
        raise HTTPException(404, "Dataset not found")
    return crud.create_analysis(db, analysis)

@app.get("/datasets/{dataset_id}/analyses", response_model=List[schemas.AnalysisResponse])
def get_dataset_analyses(dataset_id: int, db: Session = Depends(get_db)):
    return crud.get_analyses_by_dataset(db, dataset_id)

@app.get("/analysis/{analysis_id}/columns")
def get_all_columns_for_analysis(analysis_id: int, db: Session = Depends(get_db)):
    analysis = crud.get_analysis(db, analysis_id)
    if not analysis:
        raise HTTPException(404, "Analysis not found")
    dataset_id = analysis.dataset_id
    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(404, "Dataset not found")
    latest_file = crud.get_latest_file_from_s3(metadata.s3_bucket, metadata.s3_key)
    obj = boto3.client("s3").get_object(Bucket=metadata.s3_bucket, Key=latest_file)
    raw = obj["Body"].read()
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
    calc_fields = crud.get_calculated_fields_by_analysis(db, analysis_id)
    calc_field_names = [f.field_name for f in calc_fields]
    return {"columns": dataset_columns + calc_field_names}

# ---------------- Calculated fields ----------------
@app.post("/calculated-fields", response_model=schemas.CalculatedFieldOut)
def create_calc_field(payload: schemas.CalculatedFieldCreate, db: Session = Depends(get_db)):
    try:
        return crud.create_calculated_field(db, payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/analysis/{analysis_id}/calculated-fields", response_model=List[schemas.CalculatedFieldOut])
def list_calc_fields(analysis_id: int, db: Session = Depends(get_db)):
    return crud.get_calculated_fields_by_analysis(db, analysis_id)

@app.delete("/calculated-field/{field_id}")
def delete_calc_field(field_id: int, db: Session = Depends(get_db)):
    ok = crud.delete_calculated_field(db, field_id)
    if not ok:
        raise HTTPException(404, "Calculated field not found")
    return {"status": "deleted"}

# ---------------- Filters ----------------
@app.post("/filters/save", response_model=schemas.FilterResponse)
def save_filter(req: schemas.FilterSaveRequest, db: Session = Depends(get_db)):
    analysis = crud.get_analysis(db, req.analysis_id)
    if not analysis:
        raise HTTPException(404, "Analysis not found")
    if int(analysis.dataset_id) != int(req.dataset_id):
        raise HTTPException(400, "Dataset ID does not match the analysis")
    filt = crud.save_filter(db, req.dataset_id, req.analysis_id, req.selected_columns)
    return filt

@app.get("/filters/saved", response_model=schemas.FilterResponse)
def get_saved_filter(dataset_id: int = Query(...), analysis_id: int = Query(...), db: Session = Depends(get_db)):
    obj = crud.get_saved_filter(db, dataset_id, analysis_id)
    if obj is None:
        raise HTTPException(404, "No saved filter found")
    # return DB model instance - fetch the full object to include id
    rec = db.query(models.FilterSelection).filter_by(dataset_id=dataset_id, analysis_id=analysis_id).first()
    return rec

@app.delete("/filters")
def delete_filter(dataset_id: int = Query(...), analysis_id: int = Query(...), db: Session = Depends(get_db)):
    ok = crud.delete_filter(db, dataset_id, analysis_id)
    if not ok:
        raise HTTPException(404, "Filter not found")
    return {"message": "Filter deleted"}

# ---------------- Analysis Preview (Pivot) ----------------
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

    metadata = crud.get_dataset_by_id(db, dataset_id)
    if not metadata:
        raise HTTPException(404, "Dataset not found")

    obj = boto3.client("s3").get_object(Bucket=metadata.s3_bucket, Key=metadata.latest_file)
    raw = obj["Body"].read()
    if metadata.latest_file.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(raw))
    elif metadata.latest_file.endswith((".xlsx", ".xls")):
        df = pd.read_excel(io.BytesIO(raw))
    elif metadata.latest_file.endswith(".parquet"):
        df = pd.read_parquet(io.BytesIO(raw))
    else:
        raise HTTPException(400, "Unsupported file type")

    # Apply calculated fields
    calc_fields = crud.get_calculated_fields_by_analysis(db, analysis_id)
    for f in calc_fields:
        try:
            df[f.field_name] = apply_formula(df, f.formula)
            # try coerce to numeric where possible
            df[f.field_name] = pd.to_numeric(df[f.field_name], errors="ignore")
        except Exception as e:
            raise HTTPException(400, f"Formula Error in {f.field_name}: {e}")

    # Apply saved filters (can be list of cols or dict col->values)
    saved = crud.get_saved_filter(db, dataset_id, analysis_id)
    filtered_columns = []
    if saved:
        # if saved is a list -> keep those columns
        if isinstance(saved, list):
            for col in saved:
                if col in df.columns:
                    filtered_columns.append(col)
            # Keep columns: rows + cols + filtered_columns + rest
            keep_cols = list(dict.fromkeys(rows + columns + filtered_columns))
            # ensure existing columns are preserved
            keep_cols = [c for c in keep_cols if c in df.columns]
            df = df[keep_cols + [c for c in df.columns if c not in keep_cols]]
        elif isinstance(saved, dict):
            # saved dict maps column -> allowed values: apply row filtering
            for col, vals in saved.items():
                if col in df.columns and vals:
                    df = df[df[col].isin(vals)]
                    filtered_columns.append(col)
        else:
            # unsupported format -> ignore
            pass

    # Pivot
    if analysis_type == "pivot":
        agg_dict = {v.column: v.agg for v in values_config} if values_config else {}
        value_cols = [v.column for v in values_config] if values_config else []

        # add calculated fields automatically
        for f in calc_fields:
            if f.field_name not in agg_dict:
                agg_dict[f.field_name] = f.default_agg or "sum"
            if f.field_name not in value_cols:
                value_cols.append(f.field_name)

        try:
            pivot = pd.pivot_table(
                df,
                index=rows if rows else None,
                columns=columns if columns else None,
                values=value_cols if value_cols else None,
                aggfunc=agg_dict if agg_dict else "sum",
                margins=True,
                margins_name="Total"
            )
        except Exception as e:
            raise HTTPException(400, f"Pivot Error: {e}")

        pivot = pivot.reset_index()
        # flatten columns
        pivot.columns = [
            "_".join([str(x) for x in col if x not in ["", None]])
            if isinstance(col, tuple) else str(col)
            for col in pivot.columns
        ]
        # keep 'Total' row at bottom
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


# DELETE REPORT
@app.delete("/reports/{report_id}")
def delete_report(report_id: int, db: Session = Depends(get_db)):
    success = crud.delete_report(db, report_id)
    if not success:
        raise HTTPException(404, "Report not found")
    return {"status": "success", "message": "Report deleted successfully"}


# DELETE SHEET
@app.delete("/sheets/{sheet_id}")
def delete_sheet(sheet_id: int, db: Session = Depends(get_db)):
    success = crud.delete_sheet(db, sheet_id)
    if not success:
        raise HTTPException(404, "Sheet not found")
    return {"status": "success", "message": "Sheet deleted successfully"}


@app.patch("/reports/{report_id}")
def rename_report(report_id: int, payload: schemas.ReportRename, db: Session = Depends(get_db)):
    rep = crud.get_report(db, report_id)
    if not rep:
        raise HTTPException(status_code=404, detail="Report not found")
    new_name = payload.name.strip()
    if not new_name:
        raise HTTPException(status_code=400, detail="Name cannot be empty")
    # persist change
    rep.name = new_name
    db.add(rep)
    db.commit()
    db.refresh(rep)
    # return new representation consistent with your /reports/{id} response
    # For compatibility, return same structure as get_report()
    sheets = []
    for s in rep.sheets:
        s_maps = s.sheet_maps
        analyses = []
        for m in s_maps:
            a = crud.get_analysis(db, m.analysis_id)
            ds = crud.get_dataset_by_id(db, a.dataset_id)
            analyses.append({
                "analysis_id": a.id,
                "analysis_name": a.analysis_name,
                "dataset_id": ds.id,
                "dataset_name": ds.dataset_name
            })
        sheets.append({"sheet_id": s.id, "name": s.name, "analyses": analyses})
    return {"report_id": rep.id, "name": rep.name, "sheets": sheets}
