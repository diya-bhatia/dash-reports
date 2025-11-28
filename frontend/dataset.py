# dataset.py
import dash
from dash import html, dcc, Input, Output, State, ctx, no_update, dash_table, ALL
import dash_bootstrap_components as dbc
import requests
import pandas as pd
import numpy as np

API_BASE = "http://127.0.0.1:8000"

# ----------------- Create Dash App -----------------
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)
server = app.server

# ----------------- Helpers ----------------


def df_to_table_quicksight(df: pd.DataFrame, max_rows: int = 100):
    """
    Convert DataFrame -> Dash DataTable with QuickSight-style header and totals row at the bottom.
    """
    if df is None or df.empty:
        return html.Div("No rows to display", className="text-muted")

    display_df = df.head(max_rows).copy()

    # Add Total Row at the bottom
    totals = display_df.select_dtypes(include="number").sum()
    totals_row = pd.DataFrame([totals], columns=display_df.columns)
    
    # For non-numeric columns, write 'Total'
    for col in display_df.select_dtypes(exclude="number").columns:
        totals_row[col] = "Total"

    display_df = pd.concat([display_df, totals_row], ignore_index=True)

    return dash_table.DataTable(
        columns=[{"name": c, "id": c, "deletable": False, "selectable": True} for c in display_df.columns],
        data=display_df.to_dict("records"),
        # filter_action="native",
        sort_action="native",
        sort_mode="multi",
        page_action="none",
        style_table={"overflowX": "auto", "maxHeight": "500px", "overflowY": "auto"},
        style_header={
            "backgroundColor": "#670178",
            "color": "white",
            "fontWeight": "bold",
            "textAlign": "center",
            "position": "sticky",
            "top": 0,
            "zIndex": 1,
        },
        style_cell={
            "textAlign": "center",
            "minWidth": "100px",
            "width": "150px",
            "maxWidth": "300px",
            "whiteSpace": "normal",
        },
        style_data_conditional=[
            {
                "if": {"row_index": "odd"},
                "backgroundColor": "#f5f5f5",
            },
            {
                # Last row = totals
                "if": {"row_index": len(display_df)-1},
                "backgroundColor": "#670178",
                "color": "white",
                "fontWeight": "bold",
            },
        ],
        page_size=max_rows,
    )



def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def safe_post(url, json=None, timeout=15):
    try:
        r = requests.post(url, json=json, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

# ----------------- Dataset Card -----------------
def dataset_card(ds):
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H5(ds.get("dataset_name", "Unnamed"), className="fw-bold"),
                    html.Div(
                        [html.Div(["Rows: ", html.B(str(ds.get("num_rows", "N/A")))]),],
                        className="text-muted small mb-2",
                    ),
                    dbc.Button(
                        "Open",
                        id={"type": "open-dataset-btn", "dataset_id": ds["id"]},
                        color="primary",
                        size="sm",
                        className="mt-2 me-2",
                    ),
                    dbc.Button(
                        "Analysis",
                        id={"type": "analysis-btn", "dataset_id": ds["id"]},
                        color="success",
                        size="sm",
                        className="mt-2",
                    ),
                ]
            )
        ],
        className="shadow-sm",
        style={"borderRadius": "12px"},
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

        # Stores
        dcc.Store(id="current-dataset-store", data={"dataset_id": None, "page": 1, "limit": 50, "latest_file": None, "dataset_name": None}),
        dcc.Store(id="analysis-store", data={"dataset_id": None, "columns": []}),
        dcc.Store(id="preview-result-store", data={"last_preview": None}),

        html.Div(id="preview-header"),
        html.Div(id="dataset-preview", className="mt-3"),

        # Pagination controls
        dbc.Row(
            [
                dbc.Col(dbc.Button("Prev", id="prev-page", color="primary"), width="auto"),
                dbc.Col(dbc.Button("Next", id="next-page", color="primary"), width="auto"),
                dbc.Col(dbc.Input(id="goto-page", type="number", min=1, placeholder="Page #"), width=2),
                dbc.Col(dbc.Button("Go", id="goto-btn", color="secondary"), width="auto"),
                dbc.Col(
                    dcc.Dropdown(
                        id="limit-select",
                        options=[{"label": str(x), "value": x} for x in [25, 50, 100, 500]],
                        value=50,
                        clearable=False,
                        style={"width": "140px"},
                    ),
                    width="auto",
                ),
            ],
            className="mb-2 g-2 align-items-center",
        ),

        # ----------------- Analysis Modal -----------------
        dbc.Modal(
            [
                dbc.ModalHeader(dbc.ModalTitle("Create / Edit Analysis")),
                dbc.ModalBody(
                    [
                        dbc.Row(
                            [
                                dbc.Col(dbc.Label("Saved Analysis (optional)")),
                                dbc.Col(dcc.Dropdown(id="saved-analyses-select", options=[], placeholder="Choose saved analysis (optional)", clearable=True)),
                            ],
                            className="g-2 align-items-center",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(dbc.Label("Analysis Name"), width=3),
                                dbc.Col(dbc.Input(id="analysis-name", placeholder="Enter analysis name..."), width=9),
                            ],
                            className="mt-2",
                        ),
                        dbc.Row(
                            [
                                dbc.Col(dbc.Label("Analysis Type"), width=3),
                                dbc.Col(
                                    dcc.Dropdown(
                                        id="analysis-type",
                                        options=[
                                            {"label": "Pivot Table", "value": "pivot"},
                                            {"label": "Bar Chart", "value": "bar"},
                                            {"label": "Line Chart", "value": "line"},
                                            {"label": "Scatter Plot", "value": "scatter"},
                                        ],
                                        value="pivot",
                                        clearable=False,
                                    ),
                                    width=9,
                                ),
                            ],
                            className="mt-2",
                        ),

                        html.Hr(),
                        html.H5("Analysis Builder"),

                        dbc.Button("Generate Table", id="generate-table-btn", color="primary", className="mb-2"),

                        # Single scrollable container for editor + pivot output
                        html.Div(
                            id="analysis-editor-container",
                            style={
                                "maxHeight": "500px",
                                "overflowY": "auto",
                                "border": "1px solid #e6e6e6",
                                "padding": "12px",
                                "borderRadius": "10px",
                                "background": "#fafafa",
                                "boxShadow": "0 2px 8px rgba(0,0,0,0.03)",
                            },
                            children=[
                                html.Div(id="analysis-preview"),
                                html.Hr(style={"margin": "12px 0"}),
                                html.Div(id="pivot-output")
                            ]
                        ),

                        html.Div(id="analysis-msg", className="mt-2"),
                    ]
                ),
                dbc.ModalFooter(
                    dbc.Row(
                        [
                            dbc.Col(dbc.Button("Close", id="close-analysis-btn", color="secondary"), width="auto"),
                            dbc.Col(html.Div(), width=True),
                            dbc.Col(dbc.Button("Save Analysis", id="save-analysis-btn", color="success"), width="auto"),
                        ],
                        className="w-100 align-items-center",
                    )
                ),
            ],
            id="analysis-modal",
            is_open=False,
            size="lg",
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
    payload, err = safe_get(f"{API_BASE}/datasets/")
    if err:
        return html.Div("❌ Could not load datasets", className="text-danger"), f"Error: {err}"

    datasets = payload or []
    if search:
        datasets = [d for d in datasets if search.lower() in d.get("dataset_name", "").lower()]

    if not datasets:
        return html.Div("No datasets found", className="text-muted mt-4"), ""

    for d in datasets:
        d.setdefault("latest_file", "N/A")

    cards = [dbc.Col(dataset_card(ds), md=3, className="mb-3") for ds in datasets]
    return dbc.Row(cards), f"Loaded {len(datasets)} datasets"

# ----------------- Helper: fetch page from backend -----------------
def fetch_page(dataset_id, page, limit):
    try:
        res = requests.get(f"{API_BASE}/datasets/{dataset_id}/data", params={"page": page, "limit": limit}, timeout=60)
        res.raise_for_status()
        return res.json(), None
    except Exception as e:
        return None, str(e)

# ----------------- Open dataset + pagination (MERGED) -----------------
@app.callback(
    Output("current-dataset-store", "data"),
    Output("dataset-preview", "children"),
    Output("preview-header", "children"),
    Output("analysis-store", "data"),
    Input({"type": "open-dataset-btn", "dataset_id": dash.ALL}, "n_clicks"),
    Input("next-page", "n_clicks"),
    Input("prev-page", "n_clicks"),
    Input("goto-btn", "n_clicks"),
    Input("limit-select", "value"),
    State("goto-page", "value"),
    State("current-dataset-store", "data"),
    State("analysis-store", "data"),
    prevent_initial_call=True,
)
def open_and_paginate(open_clicks, next_click, prev_click, go_click, new_limit, goto_page, store, analysis_store):
    triggered = ctx.triggered_id
    if not triggered:
        return no_update, no_update, no_update, no_update

    # If user clicked an "Open" button (pattern-matching dict)
    if isinstance(triggered, dict) and triggered.get("type") == "open-dataset-btn":
        dataset_id = triggered["dataset_id"]
        page = 1
        limit = store.get("limit", 50) if store else 50
    else:
        dataset_id = store.get("dataset_id")
        if not dataset_id:
            return no_update, no_update, no_update, no_update
        page = store.get("page", 1)
        limit = store.get("limit", 50)
        if triggered == "next-page":
            page += 1
        elif triggered == "prev-page":
            page = max(1, page - 1)
        elif triggered == "goto-btn":
            try:
                if goto_page and int(goto_page) >= 1:
                    page = int(goto_page)
            except Exception:
                pass
        elif triggered == "limit-select":
            try:
                limit = int(new_limit)
            except Exception:
                pass
            page = 1

    payload, err = fetch_page(dataset_id, page, limit)
    if err:
        msg = html.Div(f"❌ Error: {err}", className="text-danger")
        return store, msg, no_update, analysis_store

    dataset_name = payload.get("dataset_name", f"Dataset {dataset_id}")
    latest_file = payload.get("latest_file", "N/A")
    total_pages = payload.get("total_pages", 1)

    df = pd.DataFrame(payload.get("data", []))
    df = df.replace({np.nan: None})

    table = df_to_table_quicksight(df)
    header = html.H4(f"Preview: {dataset_name} (Page {page} of {total_pages}) | File: {latest_file}")

    # fetch only columns for analysis-store from the backend columns API
    try:
        cols_res = requests.get(f"{API_BASE}/datasets/{dataset_id}/columns", timeout=6)
        cols_res.raise_for_status()
        columns = cols_res.json().get("columns", [])
    except Exception:
        columns = list(df.columns)

    new_store = {"dataset_id": dataset_id, "dataset_name": dataset_name, "page": page, "limit": limit, "total_pages": total_pages, "latest_file": latest_file}
    new_analysis_store = {"dataset_id": dataset_id, "columns": columns}

    return new_store, table, header, new_analysis_store

# ----------------- Build pivot editor (helper) -----------------
def build_pivot_editor_ui(cols, prefill=None):
    """
    cols: list of dataset columns
    prefill: optional dict with keys: rows, columns, values (list of dicts with column+agg)
    """
    prefill = prefill or {}
    rows_val = prefill.get("rows", [])
    cols_val = prefill.get("columns", [])
    values_val = prefill.get("values", [])

    options = [{"label": c, "value": c} for c in cols]

    # Rows + Columns
    editor_children = [
        dbc.Row(
            [
                dbc.Col(dbc.Label("Rows (Index)"), width=2),
                dbc.Col(dcc.Dropdown(id={"type": "row-field", "index": 0}, options=options, value=rows_val, multi=True), width=4),
                dbc.Col(dbc.Label("Columns"), width=2),
                dbc.Col(dcc.Dropdown(id={"type": "col-field", "index": 0}, options=options, value=cols_val, multi=True), width=4),
            ],
            className="mb-2",
        ),
        html.Hr(),
        html.Div("Values & Aggregation", className="fw-bold mb-2"),
    ]

    # Values + aggregation
    for i, v in enumerate(values_val or [{"column": None, "agg": "sum"}]):
        editor_children.append(
            dbc.Row(
                [
                    dbc.Col(dcc.Dropdown(
                        id={"type": "value-field", "index": i},
                        options=options,
                        value=v.get("column"),
                        placeholder="Select Value Column"
                    ), width=6),
                    dbc.Col(dcc.Dropdown(
                        id={"type": "agg-select", "index": i},
                        options=[
                            {"label": "sum", "value": "sum"},
                            {"label": "mean", "value": "mean"},
                            {"label": "count", "value": "count"},
                            {"label": "max", "value": "max"},
                            {"label": "min", "value": "min"},
                        ],
                        value=v.get("agg", "sum"),
                        clearable=False
                    ), width=3),
                ],
                className="mb-2",
            )
        )

    return editor_children




@app.callback(
    Output("values-container", "children"),
    Input("add-value-btn", "n_clicks"),
    Input({"type": "remove-value-btn", "index": dash.ALL}, "n_clicks"),
    State("values-container", "children"),
    State("analysis-store", "data"),
    prevent_initial_call=True
)
def modify_value_rows(add_click, remove_clicks, children, analysis_store):
    ctx_trigger = ctx.triggered_id
    cols = analysis_store.get("columns", []) if analysis_store else []
    options = [{"label": c, "value": c} for c in cols]
    agg_options = [
        {"label": "sum", "value": "sum"},
        {"label": "mean", "value": "mean"},
        {"label": "count", "value": "count"},
        {"label": "max", "value": "max"},
        {"label": "min", "value": "min"},
    ]

    if children is None:
        children = []

    # Remove button clicked
    if isinstance(ctx_trigger, dict) and ctx_trigger.get("type") == "remove-value-btn":
        idx_to_remove = ctx_trigger["index"]
        children = [child for child in children if child.id["index"] != idx_to_remove]

    # Add button clicked
    elif ctx_trigger == "add-value-btn":
        new_index = max([child.id["index"] for child in children], default=-1) + 1
        new_row = dbc.Row(
            [
                dbc.Col(html.B(f"{len(children)+1}."), width=1, style={"textAlign": "right", "paddingTop": "7px"}),
                dbc.Col(dcc.Dropdown(options=options, id={"type": "value-field", "index": new_index}, clearable=False), width=6),
                dbc.Col(dcc.Dropdown(options=agg_options, value="sum", id={"type": "agg-select", "index": new_index}, clearable=False), width=4),
                dbc.Col(dbc.Button("Remove", id={"type": "remove-value-btn", "index": new_index}, color="danger", size="sm"), width=1)
            ],
            className="mb-2",
            id={"type": "value-row", "index": new_index}
        )
        children.append(new_row)

    # Renumber rows properly
    for idx, child in enumerate(children):
        child.children[0].children = f"{idx+1}."

    return children


# ----------------- Unified Analysis Modal callback -----------------
@app.callback(
    Output("analysis-modal", "is_open"),
    Output("analysis-preview", "children"),
    Output("saved-analyses-select", "options"),
    Output("saved-analyses-select", "value"),
    Input({"type": "analysis-btn", "dataset_id": dash.ALL}, "n_clicks"),
    Input("analysis-type", "value"),
    Input("saved-analyses-select", "value"),
    Input("close-analysis-btn", "n_clicks"),
    State("analysis-store", "data"),
    prevent_initial_call=True,
)
def open_analysis_modal_and_restore(analysis_btns, analysis_type, saved_analysis_value, close_click, analysis_store):
    triggered = ctx.triggered_id
    if not triggered:
        return no_update, no_update, no_update, no_update

    # Close modal
    if triggered == "close-analysis-btn":
        return False, no_update, no_update, no_update

    # Analysis button clicked -> open modal
    if isinstance(triggered, dict) and triggered.get("type") == "analysis-btn":
        ds_id = triggered["dataset_id"]

        # Fetch dataset columns
        try:
            res = requests.get(f"{API_BASE}/datasets/{ds_id}/columns", timeout=6)
            res.raise_for_status()
            cols = res.json().get("columns", [])
        except Exception:
            cols = analysis_store.get("columns", []) if analysis_store else []

        # Fetch all saved analyses for this dataset
        saved_opts = []
        selected_analysis_id = None
        last_config = None
        try:
            res = requests.get(f"{API_BASE}/datasets/{ds_id}/analyses", timeout=6)
            res.raise_for_status()
            saved_list = res.json()

            saved_opts = [
                {"label": f"{a.get('analysis_name')} ({a.get('analysis_type')})", "value": str(a.get("id"))}
                for a in saved_list
            ]

            if saved_list:
                # Restore last saved analysis
                last_analysis = saved_list[-1]  # or sort by created_at if you have timestamp
                selected_analysis_id = str(last_analysis["id"])
                last_config = last_analysis.get("config", {})
        except Exception:
            saved_opts = []
            last_config = None

        # Build editor
        if analysis_type == "pivot":
            preview = build_pivot_editor_ui(cols, prefill=last_config)
        else:
            preview = [html.Div("Editor for other analysis types (not implemented).")]

        return True, preview, saved_opts, selected_analysis_id

    # Analysis type changed -> rebuild editor using stored columns
    if triggered == "analysis-type":
        cols = analysis_store.get("columns", []) if analysis_store else []
        if analysis_type == "pivot":
            return no_update, build_pivot_editor_ui(cols, prefill=None), no_update, no_update
        else:
            return no_update, [html.Div("Editor for other analysis types (not implemented).")], no_update, no_update

    # User selected a saved analysis -> fetch it and prefill
    if triggered == "saved-analyses-select" or (isinstance(triggered, dict) and triggered.get("type") == "saved-analyses-select"):
        saved_id = saved_analysis_value
        cols = analysis_store.get("columns", []) if analysis_store else []
        if not saved_id:
            return no_update, build_pivot_editor_ui(cols, prefill=None), no_update, no_update
        try:
            res = requests.get(f"{API_BASE}/analyses/{saved_id}", timeout=6)
            res.raise_for_status()
            saved = res.json()
            config = saved.get("config", {}) or {}
            return no_update, build_pivot_editor_ui(cols, prefill=config), no_update, no_update
        except Exception:
            return no_update, build_pivot_editor_ui(cols, prefill=None), no_update, no_update

    # Fallback
    return no_update, no_update, no_update, no_update


# ----------------- Generate Table (server-side preview) -----------------
@app.callback(
    Output("pivot-output", "children"),
    Output("preview-result-store", "data"),
    Input("generate-table-btn", "n_clicks"),
    State("analysis-type", "value"),
    State({"type": "row-field", "index": ALL}, "value"),
    State({"type": "col-field", "index": ALL}, "value"),
    State({"type": "value-field", "index": ALL}, "value"),
    State({"type": "agg-select", "index": ALL}, "value"),
    State("analysis-store", "data"),
    prevent_initial_call=True,
)
def generate_preview(n_clicks, analysis_type, row_vals, col_vals, value_cols, agg_vals, analysis_store):
    dataset_id = analysis_store.get("dataset_id") if analysis_store else None
    if not dataset_id:
        return html.Div("No dataset selected", className="text-danger"), no_update

    if analysis_type != "pivot":
        return html.Div("Preview not implemented for this analysis type", className="text-muted"), no_update

    # Flatten lists safely (handles multi=True dropdowns)
    rows_flat = []
    for r in row_vals:
        if isinstance(r, list):
            rows_flat.extend(r)
        elif r:
            rows_flat.append(r)

    cols_flat = []
    for c in col_vals:
        if isinstance(c, list):
            cols_flat.extend(c)
        elif c:
            cols_flat.append(c)

    values_flat = [v for v in value_cols if v] if value_cols else []
    aggs_flat = [a for a in agg_vals if a] if agg_vals else ["sum"]*len(values_flat)

    if not rows_flat and not cols_flat:
        return html.Div("Please select at least one Row or Column", className="text-warning"), no_update

    # Build values payload
    values_payload = []
    for i, col in enumerate(values_flat):
        agg_func = aggs_flat[i] if i < len(aggs_flat) else "sum"
        values_payload.append({"column": col, "agg": agg_func})

    payload = {
        "dataset_id": dataset_id,
        "type": "pivot",
        "rows": rows_flat,
        "columns": cols_flat,
        "values": values_payload
    }

    print("Pivot request received:")
    print("Rows:", rows_flat)
    print("Columns:", cols_flat)
    print("Values:", values_payload)

    # Call FastAPI
    res_json, err = safe_post(f"{API_BASE}/analysis/preview", json=payload, timeout=30)
    if err:
        return html.Div(f"❌ Error generating preview: {err}", className="text-danger"), no_update

    table = res_json.get("table", [])
    if not table:
        return html.Div("No rows returned by preview", className="text-muted"), {"last_preview": None}

    df = pd.DataFrame(table)
    return df_to_table_quicksight(df), {"last_preview": {"rows": len(df), "type": "pivot"}}



# ----------------- Save Analysis (sends config to backend) -----------------

@app.callback(
    Output("analysis-msg", "children"),
    Input("save-analysis-btn", "n_clicks"),
    State("analysis-name", "value"),
    State("analysis-type", "value"),
    State("analysis-store", "data"),
    State({"type": "row-field", "index": ALL}, "value"),
    State({"type": "col-field", "index": ALL}, "value"),
    State({"type": "value-field", "index": ALL}, "value"),
    State({"type": "agg-select", "index": ALL}, "value"),
    prevent_initial_call=True,
)
def save_analysis(n_clicks, name, a_type, analysis_store, rows_list, cols_list, values_list, agg_list):
    if not analysis_store or not analysis_store.get("dataset_id"):
        return html.Div("No dataset selected", className="text-danger")
    if not name or not a_type:
        return html.Div("Please provide analysis name and type", className="text-warning")

    # Flatten lists (each dropdown might be multi=True)
    rows = []
    for r in rows_list:
        if isinstance(r, list):
            rows.extend(r)
        elif r:
            rows.append(r)

    cols = []
    for c in cols_list:
        if isinstance(c, list):
            cols.extend(c)
        elif c:
            cols.append(c)

    values = values_list or []
    aggs = agg_list or ["sum"] * len(values)

    config = {}
    if a_type == "pivot":
        config = {"rows": rows, "columns": cols, "values": [{"column": v, "agg": a} for v, a in zip(values, aggs)]}
    else:
        config = {"note": "no config implemented for this type yet"}

    payload = {
        "dataset_id": analysis_store["dataset_id"],
        "analysis_name": name,
        "analysis_type": a_type,
        "config": config
    }

    try:
        res = requests.post(f"{API_BASE}/analyses/", json=payload, timeout=10)
        res.raise_for_status()
        return html.Div("✅ Analysis saved successfully!", className="text-success")
    except Exception as e:
        return html.Div(f"❌ Error saving analysis: {str(e)}", className="text-danger")


# ----------------- Run server -----------------
if __name__ == "__main__":
    app.run(debug=True, port=8050)