import dash
from dash import html, dcc, Input, Output, State, ctx, no_update
import dash_bootstrap_components as dbc
import requests
import pandas as pd
import numpy as np

API_BASE = "http://127.0.0.1:8000"

# ----------------- Create Dash App -----------------
app = dash.Dash(
    __name__, 
    external_stylesheets=[dbc.themes.BOOTSTRAP], 
    suppress_callback_exceptions=True  # because some components are dynamic
)
server = app.server

# ----------------- Dataset Card -----------------
def dataset_card(ds):
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H5(ds.get("dataset_name", "Unnamed"), className="fw-bold"),
                    html.Div(
                        [
                            html.Div(["Bucket: ", html.B(ds.get("s3_bucket", "N/A"))]),
                            html.Div(["Rows: ", html.B(str(ds.get("num_rows", "N/A")))]),
                        ],
                        className="text-muted small mb-2"
                    ),
                    dbc.Button(
                        "Open",
                        id={"type": "open-dataset-btn", "dataset_id": ds["id"]},
                        color="primary",
                        size="sm",
                        className="mt-2",
                    ),
                ]
            )
        ],
        className="shadow-sm",
        style={"borderRadius": "12px"}
    )

# ----------------- Layout -----------------
app.layout = dbc.Container(
    [
        html.H2("Datasets", className="mt-3 mb-3 fw-bold"),

        # Controls
        dbc.Row(
            [
                dbc.Col(
                    dcc.Input(id="dataset-search", placeholder="Search datasets...", className="form-control"),
                    width=4,
                ),
                dbc.Col(dbc.Button("Refresh", id="refresh-datasets", color="secondary"), width="auto"),
                dbc.Col(html.Div(id="info-msg"), width="auto"),
            ],
            className="mb-3 align-items-center",
        ),

        html.Div(id="dataset-grid"),

        html.Hr(),

        # Store selected dataset
        dcc.Store(id="current-dataset-store", data={"dataset_id": None, "page": 1, "limit": 50}),

        html.Div(id="preview-header"),
        html.Div(id="dataset-preview", className="mt-3"),

        # Pagination controls (exist upfront)
        dbc.Row(
            [
                dbc.Col(dbc.Button("Prev", id="prev-page", color="primary"), width="auto"),
                dbc.Col(dbc.Button("Next", id="next-page", color="primary"), width="auto"),
                dbc.Col(dbc.Input(id="goto-page", type="number", min=1, placeholder="Page #"), width=2),
                dbc.Col(dbc.Button("Go", id="goto-btn", color="secondary"), width="auto"),
                dbc.Col(
                    dbc.Select(
                        id="limit-select",
                        options=[{"label": str(x), "value": x} for x in [25, 50, 100, 500]],
                        value=50
                    ),
                    width=2
                ),
            ],
            className="mb-2 g-2 align-items-center",
        ),
    ],
    fluid=True,
)

# ----------------- Load dataset list -----------------
@app.callback(
    Output("dataset-grid", "children"),
    Output("info-msg", "children"),
    Input("refresh-datasets", "n_clicks"),
    Input("dataset-search", "value"),
)
def load_datasets(n_clicks, search):
    try:
        res = requests.get(f"{API_BASE}/datasets/")
        datasets = res.json()
    except Exception as e:
        return html.Div("❌ Could not load datasets", className="text-danger"), f"Error: {str(e)}"

    if search:
        datasets = [d for d in datasets if search.lower() in d.get("dataset_name", "").lower()]

    if not datasets:
        return html.Div("No datasets found", className="text-muted mt-4"), ""

    cards = [dbc.Col(dataset_card(ds), md=3, className="mb-3") for ds in datasets]
    return dbc.Row(cards), f"Loaded {len(datasets)} datasets"

# ----------------- Helper: fetch page -----------------
def fetch_page(dataset_id, page, limit):
    try:
        res = requests.get(
            f"{API_BASE}/datasets/{dataset_id}/data",
            params={"page": page, "limit": limit},
            timeout=60
        )
        res.raise_for_status()
        return res.json(), None
    except Exception as e:
        return None, str(e)

# ----------------- Single callback for open + paging -----------------
@app.callback(
    Output("current-dataset-store", "data"),
    Output("dataset-preview", "children"),
    Output("preview-header", "children"),
    Input({"type": "open-dataset-btn", "dataset_id": dash.ALL}, "n_clicks"),
    Input("next-page", "n_clicks"),
    Input("prev-page", "n_clicks"),
    Input("goto-btn", "n_clicks"),
    Input("limit-select", "value"),
    State("goto-page", "value"),
    State("current-dataset-store", "data"),
    prevent_initial_call=True,
)
def open_and_paginate(open_clicks, next_click, prev_click, go_click, new_limit, goto_page, store):
    triggered = ctx.triggered_id
    if not triggered:
        return no_update, no_update, no_update

    # ------------------ Open dataset ------------------
    if isinstance(triggered, dict) and "dataset_id" in triggered:
        dataset_id = triggered["dataset_id"]
        page = 1
        limit = store.get("limit", 50) if store else 50
    else:
        dataset_id = store.get("dataset_id")
        if not dataset_id:
            return no_update, no_update, no_update
        page = store.get("page", 1)
        limit = store.get("limit", 50)

        if triggered == "next-page":
            page += 1
        elif triggered == "prev-page":
            page = max(1, page - 1)
        elif triggered == "goto-btn":
            if goto_page and goto_page > 0:
                page = int(goto_page)
        elif triggered == "limit-select":
            limit = new_limit
            page = 1

    # Fetch dataset page
    payload, err = fetch_page(dataset_id, page, limit)
    if err:
        return store, html.Div(f"❌ Error: {err}", className="text-danger"), no_update

    dataset_name = payload.get("dataset_name", f"Dataset {dataset_id}")
    total_pages = payload.get("total_pages", 1)

    df = pd.DataFrame(payload.get("data", []))
    df = df.replace({np.nan: None})

    table = dbc.Table.from_dataframe(df, striped=True, bordered=True, hover=True) if not df.empty else html.Div("No rows")

    header = html.H4(f"Preview: {dataset_name} (Page {page} of {total_pages})")

    new_store = {
        "dataset_id": dataset_id,
        "dataset_name": dataset_name,
        "page": page,
        "limit": limit,
        "total_pages": total_pages
    }

    return new_store, table, header

# ----------------- Run server -----------------
if __name__ == "__main__":
    app.run(debug=True, port=8050)
