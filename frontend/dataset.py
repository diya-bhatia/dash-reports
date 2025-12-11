import dash
from dash import html, dcc, Input, Output, State, ctx, no_update, dash_table
import dash_bootstrap_components as dbc
import requests
import pandas as pd
import plotly.express as px
import plotly.io as pio
from datetime import datetime
import json
import base64

# ----------------- Config -----------------
API_BASE = "http://127.0.0.1:8000"
pio.templates.default = "plotly_white"

# ----------------- App init -----------------
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True
)
server = app.server
app.title = "Quick-ish Suite (Dash)"

# ----------------- Helpers (API wrappers) -----------------
def api_get(path, params=None, timeout=10):
    try:
        r = requests.get(f"{API_BASE}{path}", params=params, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_post(path, json_payload=None, timeout=15):
    try:
        r = requests.post(f"{API_BASE}{path}", json=json_payload, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

def api_patch(path, json_payload=None, timeout=15):
    try:
        r = requests.patch(f"{API_BASE}{path}", json=json_payload, timeout=timeout)
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)

# ----------------- Small UI pieces -----------------
def dataset_card(ds):
    return dbc.Card(
        dbc.CardBody(
            [
                html.H5(ds.get("dataset_name", "Unnamed"), className="fw-bold"),
                html.Small(f"Rows: {ds.get('num_rows','N/A')} • Cols: {ds.get('num_columns','N/A')}", className="text-muted d-block"),
                dbc.Button("Open", id={"type":"open-ds","id":ds["id"]}, color="primary", size="sm", className="me-2 mt-2"),
                dbc.Button("Analyses", id={"type":"analyses-ds","id":ds["id"]}, color="success", size="sm", className="mt-2"),
            ]
        ),
        className="mb-3 shadow-sm",
        style={"minHeight":"140px","borderRadius":"8px"}
    )

# ----------------- App layout -----------------
app.layout = dbc.Container(
    fluid=True,
    children=[
        dcc.Location(id="url", refresh=False),
        dbc.Row(
            [
                # Sidebar
                dbc.Col(
                    [
                        html.H4("Quick Suite (mini)", className="p-2"),
                        dcc.Input(id="search-datasets", placeholder="Search", className="form-control mb-2"),
                        dbc.Button("Load / Search", id="load-datasets-btn", color="primary", size="sm", className="mb-2"),
                        html.Hr(),
                        dbc.Nav(
                            [
                                dbc.NavLink("Home", href="/", active="exact", id="nav-home"),
                                dbc.NavLink("Datasets", href="/datasets", id="nav-datasets"),
                                dbc.NavLink("Reports", href="/reports", id="nav-reports"),
                            ],
                            vertical=True, pills=True, className="flex-column mb-3"
                        ),
                        html.Hr(),
                        html.Div(
                            [
                                html.H6("Reports", className="mt-2"),
                                dbc.Button("New Report", id="new-report-btn", color="outline-primary", size="sm"),
                                html.Div(id="report-list", className="mt-2"),
                            ],
                            style={"position":"sticky","top":"10px"}
                        ),
                    ],
                    width=2,
                    style={"background":"#fbfbfc","padding":"10px","minHeight":"100vh","borderRight":"1px solid #eee"},
                ),

                # Main
                dbc.Col(
                    [
                        html.Div(id="main-header", children=[html.H2("Home", className="mt-3 mb-3")]),

                        # Sections
                        html.Div(id="home-section", children=[html.H4("Welcome"), html.P("Select Datasets or Reports from the left.")]),

                        html.Div(id="datasets-section", style={"display":"none"}, children=[
                            html.Div(id="datasets-grid"),
                            html.Hr(),
                            html.Div(id="dataset-detail-area", children=[html.Div(id="dataset-preview-header"), html.Div(id="dataset-preview-body")]),
                        ]),

                        # Reports section - QuickSight-like card grid + report detail
                        html.Div(id="reports-section", style={"display":"none"}, children=[
                            dbc.Row(
                                [
                                    dbc.Col(dbc.Button("Refresh Reports", id="refresh-reports-btn", color="primary", size="sm"), width="auto"),
                                    dbc.Col(dbc.Button("New Report", id="new-report-btn-main", color="success", size="sm"), width="auto"),
                                ],
                                className="mb-3"
                            ),

                            # Reports listing as QuickSight-like cards
                            html.Div(id="reports-list-main", className="d-flex flex-wrap gap-3"),

                            html.Hr(),

                            # Report detail area (single sheet area shown here)
                            dbc.Row(
                                [
                                    dbc.Col(html.Div(id="report-detail"), width=8),
                                    # Side panel: now contains report-level actions (Rename) only
                                    dbc.Col(html.Div(id="report-side-panel"), width=4),
                                ]
                            ),

                            # Analyses builder (opens when a report is loaded and user clicks create)
                            html.Div(id="analyses-wrapper", children=[
                                html.Hr(),
                                html.H4("Analyses Builder", className="mt-3"),
                                html.Div(id="analyses-section", style={"display":"none"}, children=[
                                    dbc.Row([
                                        dbc.Col([
                                            html.Label("Dataset"),
                                            dcc.Dropdown(id="analysis-dataset-select", placeholder="Select a dataset"),
                                            dbc.Button("Load fields", id="load-analysis-btn", color="primary", size="sm", className="mt-2 mb-3"),
                                            html.Label("Saved Analyses"),
                                            dcc.Dropdown(id="saved-analyses-select", placeholder="Choose…", clearable=True),
                                            html.Br(),
                                            html.H6("Fields"),
                                            html.Div(id="fields-list", style={"maxHeight":"300px","overflowY":"auto","border":"1px solid #eee","padding":"6px"}),
                                        ], width=4),
                                        dbc.Col([
                                            html.H6("Builder (Rows / Columns / Values)"),
                                            html.Label("Analysis name"),
                                            dbc.Input(id="analysis-name", placeholder="Analysis name"),
                                            html.Br(),
                                            html.Label("Rows (dimension)"),
                                            dcc.Dropdown(id="builder-rows", multi=True),
                                            html.Label("Columns (dimension)"),
                                            dcc.Dropdown(id="builder-columns", multi=True),
                                            html.Label("Values (measure)"),
                                            dcc.Dropdown(id="builder-values", multi=True),
                                            html.Br(),
                                            html.Label("Chart Type"),
                                            dcc.Dropdown(id="builder-chart-type", options=[
                                                {"label":"Table (Pivot)","value":"table"},
                                                {"label":"Bar Chart","value":"bar"},
                                                {"label":"Line Chart","value":"line"},
                                                {"label":"Pie Chart","value":"pie"},
                                                {"label":"Area Chart","value":"area"},
                                            ], value="table", clearable=False),
                                            html.Br(),
                                            dbc.Button("Preview", id="analysis-preview-btn", color="primary"),
                                            dbc.Button("Save Analysis", id="save-analysis-btn", color="success", className="ms-2"),
                                            html.Hr(),
                                            html.Div(id="analysis-preview-output", style={"minHeight":"200px","border":"1px solid #eee","padding":"8px"})
                                        ], width=8)
                                    ])
                                ])
                            ])
                        ]),

                        # Rename modal (hidden by default)
                        dbc.Modal(
                            [
                                dbc.ModalHeader(dbc.ModalTitle("Rename Report")),
                                dbc.ModalBody([
                                    dbc.Label("New report name"),
                                    dbc.Input(id="rename-report-input", placeholder="Enter new report name"),
                                    html.Div(id="rename-report-feedback", className="mt-2")
                                ]),
                                dbc.ModalFooter([
                                    dbc.Button("Cancel", id="rename-report-cancel", color="secondary"),
                                    dbc.Button("Save", id="rename-report-save", color="primary"),
                                ]),
                            ],
                            id="rename-modal",
                            is_open=False,
                            size="md",
                        ),

                        # stores
                        dcc.Store(id="datasets-store"),
                        dcc.Store(id="current-dataset-store", data={"dataset_id":None,"page":1,"limit":50}),
                        dcc.Store(id="analysis-columns-store"),
                        dcc.Store(id="last-preview-store"),
                        dcc.Store(id="main-view-store", data="home"),
                        dcc.Store(id="reports-store"),
                        dcc.Store(id="current-report-store", data={"report_id": None}),
                        dcc.Store(id="report-payload-store"),
                        dcc.Store(id="open-builder-flag", data=False),
                        html.Div(id="open-builder-script", style={"display":"none"}),
                    ],
                    width=10
                )
            ]
        ),
    ],
    style={"padding":"8px"}
    )

# ----------------- Helpers -----------------
def get_triggered_json_id():
    if not ctx.triggered:
        return None
    pid = ctx.triggered[0]["prop_id"].split(".")[0]
    try:
        return json.loads(pid)
    except Exception:
        return None

# ----------------- Load datasets list -----------------
@app.callback(
    Output("datasets-grid", "children"),
    Output("datasets-store", "data"),
    Input("load-datasets-btn", "n_clicks"),
    Input("nav-datasets", "n_clicks"),
    Input("main-view-store", "data"),
    State("search-datasets", "value"),
    prevent_initial_call=True
)
def load_datasets(load_click, nav_click, view, search):
    if view and view != "datasets":
        return no_update, no_update
    if not ctx.triggered:
        return no_update, no_update
    payload, err = api_get("/datasets/")
    if err:
        return html.Div(f"Error loading datasets: {err}", className="text-danger"), {}
    datasets = payload or []
    if search:
        datasets = [d for d in datasets if search.lower() in d.get("dataset_name","").lower()]
    cards = [dbc.Col(dataset_card(ds), md=3, className="mb-3") for ds in datasets]
    grid = dbc.Row(cards) if cards else html.Div("No datasets found")
    return grid, {"datasets": datasets}

# ----------------- Load reports list -----------------
@app.callback(
    Output("reports-list-main", "children"),
    Output("reports-store", "data"),
    Input("refresh-reports-btn", "n_clicks"),
    Input("nav-reports", "n_clicks"),
    Input("main-view-store", "data"),
    Input("new-report-btn-main", "n_clicks"),
    prevent_initial_call=True
)
def load_reports(refresh_click, nav_click, view, new_click):
    if view and view != "reports":
        return no_update, no_update
    if not ctx.triggered:
        return no_update, no_update
    payload, err = api_get("/reports/")
    if err:
        return html.Div(f"Error loading reports: {err}", className="text-danger"), no_update
    reports = payload or []
    cards = []
    for r in reports:
        created = r.get("created_at", "")
        cards.append(
            dbc.Card(
                [
                    html.Div(style={"height":"110px","background":"#f5f5f7","display":"flex","alignItems":"center","justifyContent":"center"}, children=[html.Span("Preview", className="text-muted")]),
                    dbc.CardBody([html.H5(r.get("name","Unnamed"), className="card-title"), html.Small(created, className="text-muted d-block mb-2"), dbc.Button("Open", id={"type":"open-report","id": r.get("id")}, size="sm", color="primary")])
                ],
                style={"width":"240px","margin":"8px"}
            )
        )
    return cards, {"reports": reports}

# ----------------- Switch main view -----------------
@app.callback(
    Output("main-view-store", "data"),
    Output("current-report-store", "data"),
    Input("url", "pathname"),
)
def switch_main_view(pathname):
    if pathname == "/datasets":
        return "datasets", {"report_id": None}
    if pathname == "/reports":
        return "reports", {"report_id": None}
    return "home", {"report_id": None}

# Toggle sections visibility
@app.callback(
    Output("home-section", "style"),
    Output("datasets-section", "style"),
    Output("reports-section", "style"),
    Output("main-header", "children"),
    Input("main-view-store", "data"),
)
def toggle_sections(view):
    if view == "reports":
        return {"display":"none"}, {"display":"none"}, {"display":"block"}, [html.H2("Reports", className="mt-3 mb-3")]
    if view == "datasets":
        return {"display":"none"}, {"display":"block"}, {"display":"none"}, [html.H2("Datasets", className="mt-3 mb-3")]
    return {"display":"block"}, {"display":"none"}, {"display":"none"}, [html.H2("Home", className="mt-3 mb-3")]

# ----------------- Open report: write to report-payload-store -----------------
@app.callback(
    Output("report-payload-store", "data", allow_duplicate=True),
    Output("current-report-store", "data", allow_duplicate=True),
    Input({"type":"open-report","id": dash.ALL}, "n_clicks"),
    prevent_initial_call=True
)
def open_report_detail(n_list):
    if not ctx.triggered:
        return no_update, no_update
    triggered_id = get_triggered_json_id()
    if not triggered_id or triggered_id.get("type") != "open-report":
        return no_update, no_update
    report_id = triggered_id.get("id")
    res, err = api_get(f"/reports/{report_id}")
    if err:
        return {"error": str(err)}, {"report_id": None}
    return res, {"report_id": report_id}

# ----------------- Render report detail from report-payload-store -----------------
@app.callback(
    Output("report-detail", "children"),
    Output("report-side-panel", "children"),
    Input("report-payload-store", "data"),
    prevent_initial_call=True
)
def render_report_from_store(payload):
    if not payload:
        return no_update, no_update
    if isinstance(payload, dict) and payload.get("error"):
        return html.Div(f"Error loading report: {payload.get('error')}", className="text-danger"), no_update
    report_payload = payload
    title = report_payload.get("name","Report")
    sheets = report_payload.get("sheets", []) or []

    # Main report detail: single sheet area shows each sheet with its controls (only here)
    sheet_blocks = []
    for s in sheets:
        sid = s.get("sheet_id")
        analyses = s.get("analyses", []) or []
        items = [html.Div([html.B(a.get("analysis_name")), html.Div(f"Dataset: {a.get('dataset_name','N/A')}")], style={"padding":"6px","border":"1px solid #eee","marginBottom":"6px"}) for a in analyses]
        sheet_blocks.append(
            html.Div(
                dbc.Card(
                    dbc.CardBody([
                        html.H5(s.get("name","Sheet")),
                        html.Div(items),
                        dbc.Row([dbc.Col(dcc.Dropdown(id={"type":"sheet-dataset","sheet":sid}, placeholder="Select dataset"), width=6),
                                 dbc.Col(dcc.Dropdown(id={"type":"sheet-analysis","sheet":sid}, placeholder="Select analysis"), width=6)]),
                        html.Div(className="mt-2", children=[
                            dbc.Button("Create Analysis", id={"type":"create-analysis-sheet","sheet":sid}, color="outline-primary", size="sm", className="me-2"),
                            dbc.Button("Add Analysis", id={"type":"add-analysis-sheet","sheet":sid}, color="primary", size="sm")
                        ])
                    ]),
                ),
                id=f"sheet-{sid}",
                style={"marginBottom":"12px"}
            )
        )

    detail = html.Div([html.H3(title), html.Hr(), html.Div(sheet_blocks)])

    # Side panel: report-level actions (Rename)
    side_panel = html.Div(
        [
            html.H5("Report Actions"),
            html.Div([html.Small(f"Report ID: {report_payload.get('id','')}")], className="mb-2"),
            dbc.Button("Rename Report", id="rename-report-btn", color="outline-primary", size="sm", className="mb-2"),
            dbc.Button("Refresh Report", id="refresh-report-btn", color="secondary", size="sm"),
        ],
        style={"padding":"8px","border":"1px solid #eee","borderRadius":"8px"}
    )

    return detail, side_panel

# ----------------- Hydrate sheet dataset dropdowns -----------------
@app.callback(
    Output({"type":"sheet-dataset","sheet": dash.ALL}, "options"),
    Input("report-detail", "children"),
    prevent_initial_call=True
)
def hydrate_sheet_dropdowns(report_detail_children):
    if report_detail_children is None:
        return no_update
    ds_payload, err = api_get("/datasets/")
    if err or not isinstance(ds_payload, list):
        opts = []
    else:
        opts = [{"label": d.get("dataset_name",""), "value": d.get("id")} for d in ds_payload]
    return [opts]

# ----------------- Hydrate analysis dataset dropdown (options only) -----------------
@app.callback(
    Output("analysis-dataset-select", "options"),
    Input("datasets-store", "data"),
    prevent_initial_call=True
)
def hydrate_analysis_dataset_dropdown(ds_store):
    if not ds_store:
        return no_update
    return [{"label": d.get("dataset_name",""), "value": d.get("id")} for d in ds_store.get("datasets", [])]

# ----------------- Load fields (single writer for builder population) -----------------
@app.callback(
    Output("fields-list", "children"),
    Output("builder-rows", "options"),
    Output("builder-columns", "options"),
    Output("builder-values", "options"),
    Output("saved-analyses-select", "options"),
    Output("analysis-columns-store", "data"),
    Output("analyses-section", "style"),
    Input("load-analysis-btn", "n_clicks"),
    Input("analysis-dataset-select", "value"),
    State("analysis-dataset-select", "value"),
    prevent_initial_call=True
)
def load_analysis_panel(load_click, ds_value_trigger, ds_state_value):
    ds_id = ds_state_value or ds_value_trigger
    if not ds_id:
        return html.Div("Select a dataset first to load fields", className="text-muted"), [], [], [], [], {"columns": []}, {"display":"none"}
    cols_res, err_cols = api_get(f"/datasets/{ds_id}/columns")
    cols = cols_res.get("columns", []) if not err_cols else []
    chips = [html.Div(c, className="p-1 mb-1", style={"border":"1px solid #ddd","padding":"6px","borderRadius":"6px","display":"inline-block","marginRight":"6px"}) for c in cols]
    options = [{"label": c, "value": c} for c in cols]
    saved_opts = []
    saved_res, err = api_get(f"/datasets/{ds_id}/analyses")
    if not err and isinstance(saved_res, list):
        saved_opts = [{"label": f"{a.get('analysis_name','')} ({a.get('analysis_type','')})", "value": str(a.get("id"))} for a in saved_res]
    return chips, options, options, options, saved_opts, {"columns": cols}, {"display":"block"}

# ----------------- When user clicks Create Analysis -> set dataset value and open builder -----------------
@app.callback(
    Output("analysis-dataset-select", "value"),
    Output("open-builder-flag", "data", allow_duplicate=True),
    Input({"type":"create-analysis-sheet","sheet": dash.ALL}, "n_clicks"),
    State({"type":"sheet-dataset","sheet": dash.ALL}, "value"),
    State("datasets-store", "data"),
    prevent_initial_call=True
)
def create_analysis_from_sheet(n_list, sheet_dataset_vals, datasets_store):
    if not ctx.triggered:
        return no_update, no_update
    triggered = get_triggered_json_id()
    if not triggered or triggered.get("type") != "create-analysis-sheet":
        return no_update, no_update
    # Best-effort: pick dataset from sheet_dataset_vals if present, otherwise first dataset in store
    ds_id = None
    if sheet_dataset_vals:
        for v in sheet_dataset_vals:
            if v:
                ds_id = v
                break
    if not ds_id:
        ds_list = (datasets_store or {}).get("datasets", []) or []
        if ds_list:
            ds_id = ds_list[0].get("id")
    if not ds_id:
        return no_update, no_update
    return ds_id, True

# ----------------- Add saved analysis to sheet (writes updated report payload to store) -----------------
@app.callback(
    Output("report-payload-store", "data", allow_duplicate=True),
    Input({"type":"add-analysis-sheet","sheet": dash.ALL}, "n_clicks"),
    State({"type":"sheet-dataset","sheet": dash.ALL}, "value"),
    State({"type":"sheet-analysis","sheet": dash.ALL}, "value"),
    State("current-report-store", "data"),
    State({"type":"add-analysis-sheet","sheet": dash.ALL}, "id"),
    prevent_initial_call=True
)
def add_analysis_to_sheet(n_list, dataset_ids, analysis_ids, report_store, btn_ids):
    if not ctx.triggered:
        return no_update
    if not report_store or not report_store.get("report_id"):
        return {"error": "Open a report first"}
    triggered = get_triggered_json_id()
    if not triggered or triggered.get("type") != "add-analysis-sheet":
        return no_update
    sheet_id = triggered.get("sheet")
    idx = None
    for i, bid in enumerate(btn_ids or []):
        if bid and bid.get("sheet") == sheet_id:
            idx = i
            break
    if idx is None:
        return {"error":"Sheet not found"}
    analysis_id = (analysis_ids or [None])[idx]
    if not analysis_id:
        return {"error":"Select an analysis first"}
    res, err = api_post(f"/sheets/{sheet_id}/add-analysis", json_payload={"analysis_id": analysis_id})
    if err:
        return {"error": str(err)}
    rep_res, rep_err = api_get(f"/reports/{report_store['report_id']}")
    if rep_err:
        return {"error": str(rep_err)}
    return rep_res

# ----------------- Rename report modal control + rename action -----------------
@app.callback(
    Output("rename-modal", "is_open"),
    Output("report-payload-store", "data", allow_duplicate=True),
    Output("reports-list-main", "children", allow_duplicate=True),
    Output("reports-store", "data", allow_duplicate=True),
    Input("rename-report-btn", "n_clicks"),
    Input("rename-report-save", "n_clicks"),
    Input("rename-report-cancel", "n_clicks"),
    State("rename-report-input", "value"),
    State("current-report-store", "data"),
    prevent_initial_call=True
)
def handle_rename_modal(open_btn, save_btn, cancel_btn, new_name, current_report):
    if not ctx.triggered:
        return no_update, no_update, no_update, no_update

    triggered = ctx.triggered[0]["prop_id"].split(".")[0]

    # Open modal
    if triggered == "rename-report-btn":
        return True, no_update, no_update, no_update

    # Cancel closes modal
    if triggered == "rename-report-cancel":
        return False, no_update, no_update, no_update

    # Save: attempt to persist rename to backend using PATCH /reports/{id}
    if triggered == "rename-report-save":
        if not current_report or not current_report.get("report_id"):
            return False, {"error": "No report open"}, no_update, no_update
        report_id = current_report["report_id"]

        if not new_name or str(new_name).strip() == "":
            return False, {"error": "Name cannot be empty"}, no_update, no_update

        # Try server-side PATCH /reports/{id}
        payload = {"name": new_name}
        res_patch, err_patch = api_patch(f"/reports/{report_id}", json_payload=payload)
        if err_patch is None and res_patch:
            # successful server rename; reload reports list and the report payload from server
            rep_res, rep_err = api_get(f"/reports/{report_id}")
            reps_res, reps_err = api_get("/reports/")
            if rep_err:
                # renamed but failed to reload single report — still close modal and update list if available
                reports_cards = []
                reports_store = {"reports": reps_res} if reps_res else None
                if reps_res:
                    for r in reps_res:
                        reports_cards.append(
                            dbc.Card(
                                [
                                    html.Div(style={"height":"110px","background":"#f5f5f7","display":"flex","alignItems":"center","justifyContent":"center"}, children=[html.Span("Preview", className="text-muted")]),
                                    dbc.CardBody([html.H5(r.get("name","Unnamed"), className="card-title"), dbc.Button("Open", id={"type":"open-report","id": r.get("id")}, size="sm", color="primary")])
                                ],
                                style={"width":"240px","margin":"8px"}
                            )
                        )
                return False, rep_res if rep_res else {"error": "Renamed but failed to reload report"}, reports_cards, reports_store

            # build reports cards UI for immediate refresh
            reports_cards = []
            reports_store = {"reports": reps_res} if reps_res else {"reports": []}
            for r in (reps_res or []):
                reports_cards.append(
                    dbc.Card(
                        [
                            html.Div(style={"height":"110px","background":"#f5f5f7","display":"flex","alignItems":"center","justifyContent":"center"}, children=[html.Span("Preview", className="text-muted")]),
                            dbc.CardBody([html.H5(r.get("name","Unnamed"), className="card-title"), html.Small(r.get("created_at",""), className="text-muted d-block mb-2"), dbc.Button("Open", id={"type":"open-report","id": r.get("id")}, size="sm", color="primary")])
                        ],
                        style={"width":"240px","margin":"8px"}
                    )
                )
            return False, rep_res, reports_cards, reports_store

        # If server rename failed (endpoint not present or returned error), do client-side rename fallback
        # Fetch current payload and mutate it (non-persistent).
        rep_res, rep_err = api_get(f"/reports/{report_id}")
        reps_res, reps_err = api_get("/reports/")
        if rep_err:
            return False, {"error": f"Rename not supported and reload failed: {rep_err}"}, no_update, no_update

        # mutate local payload
        rep_res["name"] = new_name

        # Build updated reports cards to reflect new name client-side (won't persist)
        reports_cards = []
        reports_list = reps_res or []
        # update the item in reports_list if present
        for r in reports_list:
            if r.get("id") == report_id:
                r["name"] = new_name
        for r in reports_list:
            reports_cards.append(
                dbc.Card(
                    [
                        html.Div(style={"height":"110px","background":"#f5f5f7","display":"flex","alignItems":"center","justifyContent":"center"}, children=[html.Span("Preview", className="text-muted")]),
                        dbc.CardBody([html.H5(r.get("name","Unnamed"), className="card-title"), dbc.Button("Open", id={"type":"open-report","id": r.get("id")}, size="sm", color="primary")])
                    ],
                    style={"width":"240px","margin":"8px"}
                )
            )
        return False, rep_res, reports_cards, {"reports": reports_list}

    return no_update, no_update, no_update, no_update

# ----------------- Preview & Save Analysis -----------------
@app.callback(
    Output("analysis-preview-output", "children"),
    Output("last-preview-store", "data"),
    Input("analysis-preview-btn", "n_clicks"),
    State("builder-rows", "value"),
    State("builder-columns", "value"),
    State("builder-values", "value"),
    State("builder-chart-type", "value"),
    State("analysis-dataset-select", "value"),
    prevent_initial_call=True
)
def run_preview(n, rows, cols, values, chart_type, dataset_id):
    if not dataset_id:
        return html.Div("Select a dataset first", className="text-warning"), no_update
    payload = {"dataset_id": dataset_id, "analysis_id": 1, "type": "pivot", "rows": rows or [], "columns": cols or [], "values": [{"column":v,"agg":"sum"} for v in (values or [])]}
    res, err = api_post("/analysis/preview", json_payload=payload)
    if err:
        return html.Div(f"Preview error: {err}", className="text-danger"), no_update
    table = res.get("table", [])
    df = pd.DataFrame(table)
    if chart_type == "table" or df.empty:
        dt = dash_table.DataTable(columns=[{"name":c,"id":c} for c in df.columns], data=df.to_dict("records"), page_size=10, style_table={"overflowX":"auto"})
        return dt, {"last_preview": {"rows": len(df), "type":"table"}}
    try:
        if chart_type == "bar" and len(rows)>0 and len(values)>0:
            fig = px.bar(df, x=rows[0], y=values[0], title="Bar chart")
        elif chart_type == "line" and len(rows)>0 and len(values)>0:
            fig = px.line(df, x=rows[0], y=values[0], title="Line chart")
        elif chart_type == "pie" and len(rows)>0 and len(values)>0:
            fig = px.pie(df, names=rows[0], values=values[0], title="Pie chart")
        elif chart_type == "area" and len(rows)>0 and len(values)>0:
            fig = px.area(df, x=rows[0], y=values[0], title="Area chart")
        else:
            return html.Div("Please select Rows and Values for charts"), no_update
        return dcc.Graph(figure=fig, config={"displayModeBar":True}), {"last_preview": {"rows": len(df), "type":"chart"}}
    except Exception as e:
        return html.Div(f"Chart render error: {str(e)}", className="text-danger"), no_update

@app.callback(
    Output("analysis-preview-output", "children", allow_duplicate=True),
    Input("save-analysis-btn", "n_clicks"),
    State("analysis-name", "value"),
    State("builder-rows", "value"),
    State("builder-columns", "value"),
    State("builder-values", "value"),
    State("builder-chart-type", "value"),
    State("analysis-dataset-select", "value"),
    prevent_initial_call=True
)
def save_analysis(n, name, rows, cols, values, chart_type, dataset_id):
    if not name:
        return html.Div("Please enter analysis name", className="text-warning"), no_update
    if not dataset_id:
        return html.Div("Select a dataset first", className="text-warning"), no_update
    payload = {"dataset_id": dataset_id, "analysis_name": name, "analysis_type": chart_type or "pivot", "config": {"rows": rows or [], "columns": cols or [], "values": [{"column":v,"agg":"sum"} for v in (values or [])]}}
    res, err = api_post("/analyses/", json_payload=payload)
    if err:
        return html.Div(f"Error saving analysis: {err}", className="text-danger"), no_update
    return html.Div("Saved analysis successfully ✅", className="text-success"), no_update

# ----------------- Export dataset CSV -----------------
@app.callback(
    Output("dataset-preview-body", "children", allow_duplicate=True),
    Input("export-ds-csv", "n_clicks"),
    State("current-dataset-store", "data"),
    prevent_initial_call=True
)
def export_dataset_csv(n, dataset_store):
    if not dataset_store or not dataset_store.get("dataset_id"):
        return no_update
    ds_id = dataset_store["dataset_id"]
    page = dataset_store.get("page",1)
    limit = dataset_store.get("limit",50)
    res, err = api_get(f"/datasets/{ds_id}/data", params={"page":page, "limit":limit})
    if err:
        return html.Div(f"Export error: {err}", className="text-danger")
    df = pd.DataFrame(res.get("data",[]))
    csv_string = df.to_csv(index=False, encoding='utf-8')
    csv_b64 = base64.b64encode(csv_string.encode()).decode()
    csv_string_b64 = "data:text/csv;base64," + csv_b64
    download_link = html.A("📥 Download CSV", href=csv_string_b64, download=f"{dataset_store.get('dataset_name','dataset')}_page{page}.csv", className="btn btn-success mt-2")
    return html.Div([download_link], className="mt-3")

# ----------------- Run server -----------------
if __name__ == "__main__":
    app.run(debug=True, port=8050)