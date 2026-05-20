"""Submit — inject new chart entries into the streaming pipeline via Kafka.

Single entry: fill the form and publish one row.
Bulk JSON: paste an array of objects to publish many rows at once.

Each submission is sent to the `charts-feed` Kafka topic. The Spark consumer
picks it up in the next micro-batch (~10 s), runs Bronze → Silver → Gold, and
the "Recently Processed" feed below refreshes automatically every 15 s.

Requires the streaming stack: make streaming-up
"""
import hashlib
import json
import os
from datetime import date

import dash
import dash_table
from dash import Input, Output, State, callback, ctx, dcc, html

from dashboard.components.cards import section_header
from dashboard.components.shell import page_hero, topbar
from dashboard.theme import ACCENT

dash.register_page(__name__, path="/submit", title="Submit Entry")

REGIONS = [
    "global", "us", "gb", "fr", "de", "es", "it", "nl", "be", "se",
    "no", "dk", "fi", "br", "mx", "ar", "cl", "co", "au", "nz",
    "jp", "kr", "in", "za", "ng", "eg", "tr", "pl", "cz", "hu",
]

# ── Shared style tokens ────────────────────────────────────────────────────────

_inp = {
    "width": "100%",
    "padding": "10px 14px",
    "background": "var(--res-surface-2)",
    "border": "1px solid var(--res-border)",
    "borderRadius": "10px",
    "color": "var(--res-text)",
    "fontSize": "14px",
    "outline": "none",
    "boxSizing": "border-box",
}

_label = {
    "fontSize": "12px",
    "fontWeight": "600",
    "color": "var(--res-text-secondary)",
    "textTransform": "uppercase",
    "letterSpacing": "0.05em",
    "marginBottom": "6px",
    "display": "block",
}

_btn = {
    "background": ACCENT, "color": "#fff", "border": "none",
    "borderRadius": "10px", "padding": "12px 28px",
    "fontSize": "14px", "fontWeight": "600", "cursor": "pointer",
    "marginTop": "8px", "letterSpacing": "0.01em",
}

_card = {
    "flex": "1", "minWidth": "0",
    "background": "var(--res-surface-1)",
    "border": "1px solid var(--res-border)",
    "borderRadius": "16px",
    "padding": "28px 32px",
}


# ── Component helpers ──────────────────────────────────────────────────────────

def _field(label_text: str, control) -> html.Div:
    return html.Div([
        html.Label(label_text, style=_label),
        control,
    ], style={"marginBottom": "18px"})


def _step(num: str, label: str, desc: str) -> html.Div:
    return html.Div([
        html.Div([
            html.Div(num, style={
                "width": "22px", "height": "22px", "borderRadius": "50%",
                "background": ACCENT, "color": "#fff",
                "display": "grid", "placeItems": "center",
                "fontSize": "11px", "fontWeight": "700", "flexShrink": "0",
            }),
            html.Div([
                html.Span(label, style={"fontWeight": "600", "fontSize": "13px",
                                        "color": "var(--res-text)"}),
                html.Span(f" — {desc}", style={"fontSize": "12px",
                                                     "color": "var(--res-text-tertiary)"}),
            ]),
        ], style={"display": "flex", "gap": "10px", "alignItems": "center"}),
    ], style={"marginBottom": "10px"})


# ── Static sub-layouts ─────────────────────────────────────────────────────────

_info_panel = html.Div([
    html.Div("How it works", style={
        "fontSize": "13px", "fontWeight": "700",
        "color": "var(--res-text)", "marginBottom": "14px",
    }),
    _step("1", "Submit", "Row(s) published to Kafka charts-feed"),
    _step("2", "~10 s", "Spark micro-batch picks up the message"),
    _step("3", "Bronze", "Raw row appended to bronze/charts_streaming"),
    _step("4", "Silver", "Cleaning + MERGE into silver/charts_streaming"),
    _step("5", "Gold", "Top-artists aggregate recomputed"),
    _step("6", "~30 s", "Recent feed below refreshes with your entry"),
], style={
    "background": "var(--res-surface-1)",
    "border": "1px solid var(--res-border)",
    "borderRadius": "16px",
    "padding": "24px 28px",
    "alignSelf": "flex-start",
    "minWidth": "230px",
})

_json_placeholder = json.dumps([
    {
        "artist": "Taylor Swift", "title": "Anti-Hero",
        "region": "us", "chart": "top200", "rank": 1,
        "date": str(date.today()),
    },
    {
        "artist": "Ed Sheeran", "title": "Shape of You",
        "region": "gb", "chart": "top200", "rank": 3,
        "streams": 1234567, "date": str(date.today()),
    },
], indent=2)


# ── Layout ─────────────────────────────────────────────────────────────────────

layout = html.Div([
    topbar("Submit Entry"),
    dcc.Interval(id="recent-refresh", interval=15_000, n_intervals=0),

    html.Div([
        page_hero(
            eyebrow="Live Ingestion",
            title="Add chart entries to the stream",
            subtitle=(
                "Submissions go to Kafka → Spark → Delta → "
                "dashboard live feed in ~40 seconds."
            ),
        ),

        section_header(
            "New Chart Entry",
            meta="Published to Kafka · charts-feed topic",
        ),

        # ── Tab bar ───────────────────────────────────────────────────────
        html.Div([
            html.Button("Single Entry", id="tab-single", n_clicks=0,
                        className="res-seg-btn active"),
            html.Button("Bulk JSON", id="tab-bulk", n_clicks=0,
                        className="res-seg-btn"),
        ], className="res-seg-wrap", style={"marginBottom": "24px"}),

        # ── Form card + info panel ─────────────────────────────────────────
        html.Div([

            html.Div([

                # ── Single-entry panel ─────────────────────────────────────
                html.Div([
                    _field("Artist *", dcc.Input(
                        id="sub-artist", type="text",
                        placeholder="e.g. Taylor Swift",
                        debounce=False, style=_inp,
                    )),
                    _field("Track Title *", dcc.Input(
                        id="sub-title", type="text",
                        placeholder="e.g. Anti-Hero",
                        debounce=False, style=_inp,
                    )),
                    html.Div([
                        html.Div([
                            _field("Region *", dcc.Dropdown(
                                id="sub-region",
                                options=[{"label": r.upper(), "value": r} for r in REGIONS],
                                value="global", clearable=False,
                                style={"fontSize": "14px"},
                            )),
                        ], style={"flex": "1"}),
                        html.Div([
                            _field("Chart *", dcc.Dropdown(
                                id="sub-chart",
                                options=[
                                    {"label": "Top 200", "value": "top200"},
                                    {"label": "Viral 50", "value": "viral50"},
                                ],
                                value="top200", clearable=False,
                                style={"fontSize": "14px"},
                            )),
                        ], style={"flex": "1"}),
                    ], style={"display": "flex", "gap": "16px"}),

                    html.Div([
                        html.Div([
                            _field("Rank (1–200) *", dcc.Input(
                                id="sub-rank", type="number", min=1, max=200,
                                placeholder="1",
                                style=_inp, className="res-no-spin",
                            )),
                        ], style={"flex": "1"}),
                        html.Div([
                            _field("Streams (optional)", dcc.Input(
                                id="sub-streams", type="number", min=0,
                                placeholder="leave blank for Viral 50",
                                style=_inp, className="res-no-spin",
                            )),
                        ], style={"flex": "2"}),
                    ], style={"display": "flex", "gap": "16px"}),

                    _field("Date *", dcc.Input(
                        id="sub-date", type="date",
                        value=str(date.today()),
                        style=_inp,
                    )),

                    html.Button("Publish to Kafka →", id="sub-submit",
                                n_clicks=0, style=_btn),
                    html.Div(id="sub-feedback", style={"marginTop": "20px"}),
                ], id="panel-single"),

                # ── Bulk JSON panel (hidden by default) ────────────────────
                html.Div([
                    html.P(
                        "Paste a JSON array of chart entries. Required fields: "
                        "artist, title, region, chart, rank, date. "
                        "streams is optional.",
                        style={"marginBottom": "14px"},
                    ),
                    _field("JSON Payload *", dcc.Textarea(
                        id="bulk-json",
                        placeholder=_json_placeholder,
                        style={
                            **_inp,
                            "height": "220px",
                            "resize": "vertical",
                            "fontFamily": "var(--res-font-mono)",
                            "fontSize": "12.5px",
                            "lineHeight": "1.55",
                        },
                    )),
                    html.Button("Publish All to Kafka →", id="bulk-submit",
                                n_clicks=0, style=_btn),
                    html.Div(id="bulk-feedback", style={"marginTop": "20px"}),
                ], id="panel-bulk", style={"display": "none"}),

            ], style=_card),

            _info_panel,

        ], style={"display": "flex", "gap": "24px", "alignItems": "flex-start"}),

        # ── Recent submissions ─────────────────────────────────────────────
        section_header(
            "Recently Processed",
            meta="Auto-refreshes every 15 s · sourced from gold/recent_submissions",
        ),
        html.Div(id="recent-submissions"),

    ], className="res-content"),
])


# ── Callbacks ──────────────────────────────────────────────────────────────────

@callback(
    Output("panel-single", "style"),
    Output("panel-bulk", "style"),
    Output("tab-single", "className"),
    Output("tab-bulk", "className"),
    Input("tab-single", "n_clicks"),
    Input("tab-bulk", "n_clicks"),
    prevent_initial_call=True,
)
def _switch_tab(_n_single, _n_bulk):
    if ctx.triggered_id == "tab-bulk":
        return {"display": "none"}, {}, "res-seg-btn", "res-seg-btn active"
    return {}, {"display": "none"}, "res-seg-btn active", "res-seg-btn"


@callback(
    Output("sub-feedback", "children"),
    Input("sub-submit", "n_clicks"),
    State("sub-artist",  "value"),
    State("sub-title",   "value"),
    State("sub-region",  "value"),
    State("sub-chart",   "value"),
    State("sub-rank",    "value"),
    State("sub-streams", "value"),
    State("sub-date",    "value"),
    prevent_initial_call=True,
)
def publish_single(n_clicks, artist, title, region, chart, rank, streams, entry_date):
    missing = [f for f, v in [("Artist", artist), ("Title", title),
                               ("Region", region), ("Rank", rank),
                               ("Date", entry_date)] if not v]
    if missing:
        return _feedback(f"Missing required fields: {', '.join(missing)}", ok=False)

    row = _build_row(artist, title, region, chart, rank, streams, entry_date)
    return _kafka_send([row], f'"{title}" by {artist} ({region.upper()}, rank #{rank})')


@callback(
    Output("bulk-feedback", "children"),
    Input("bulk-submit", "n_clicks"),
    State("bulk-json", "value"),
    prevent_initial_call=True,
)
def publish_bulk(n_clicks, json_text):
    if not json_text or not json_text.strip():
        return _feedback("Paste a JSON array before submitting.", ok=False)

    try:
        entries = json.loads(json_text)
    except json.JSONDecodeError as exc:
        return _feedback(f"Invalid JSON: {exc}", ok=False)

    if not isinstance(entries, list):
        return _feedback("JSON must be an array [ ... ] of objects.", ok=False)

    required = {"artist", "title", "region", "chart", "rank", "date"}
    rows = []
    for i, entry in enumerate(entries):
        if not isinstance(entry, dict):
            return _feedback(f"Entry {i + 1} is not an object.", ok=False)
        missing = required - set(entry.keys())
        if missing:
            return _feedback(
                f"Entry {i + 1} missing: {', '.join(sorted(missing))}", ok=False
            )
        rows.append(_build_row(
            entry["artist"], entry["title"], entry["region"],
            entry.get("chart", "top200"), entry["rank"],
            entry.get("streams"), entry["date"],
        ))

    return _kafka_send(rows, f"{len(rows)} entr{'y' if len(rows) == 1 else 'ies'}")


@callback(
    Output("recent-submissions", "children"),
    Input("recent-refresh", "n_intervals"),
)
def update_recent(_n):
    from dashboard.data_loader import available_tables, load_gold

    if "recent_submissions" not in available_tables():
        return html.Div(
            "No submissions processed yet — start the streaming stack "
            "and publish an entry above.",
            style={"color": "var(--res-text-tertiary)", "fontSize": "13px",
                   "padding": "16px 0"},
        )

    load_gold.cache_clear()
    df = load_gold("recent_submissions")

    if df.empty:
        return html.Div("No entries processed yet.",
                        style={"color": "var(--res-text-tertiary)", "fontSize": "13px"})

    df = df.sort_values("ingestion_ts", ascending=False).head(20).copy()

    if "ingestion_ts" in df.columns:
        df["ingestion_ts"] = df["ingestion_ts"].astype(str).str[:19]
    if "chart_date" in df.columns:
        df["chart_date"] = df["chart_date"].astype(str)
    if "streams" in df.columns:
        df["streams"] = df["streams"].apply(
            lambda x: f"{int(x):,}" if x is not None and str(x) not in ("", "nan", "<NA>") else "—"
        )
    if "rank" in df.columns:
        df["rank"] = df["rank"].apply(
            lambda x: f"#{int(x)}" if x is not None and str(x) not in ("", "nan", "<NA>") else "—"
        )

    cols_order = ["ingestion_ts", "artist", "title", "region", "chart", "rank", "streams", "chart_date"]
    display_cols = [c for c in cols_order if c in df.columns]
    col_labels = {
        "ingestion_ts": "Processed At",
        "artist": "Artist",
        "title": "Title",
        "region": "Region",
        "chart": "Chart",
        "rank": "Rank",
        "streams": "Streams",
        "chart_date": "Chart Date",
    }

    return html.Div(
        dash_table.DataTable(
            data=df[display_cols].to_dict("records"),
            columns=[{"name": col_labels.get(c, c), "id": c} for c in display_cols],
            page_size=10,
            style_table={"overflowX": "auto"},
            style_cell={"whiteSpace": "nowrap"},
        ),
        className="res-table",
    )


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _build_row(artist, title, region, chart, rank, streams, entry_date) -> dict:
    fake_id = hashlib.md5(f"{artist}:{title}".encode()).hexdigest()[:22]
    return {
        "title":   str(title),
        "rank":    str(int(rank)),
        "date":    str(entry_date),
        "artist":  str(artist),
        "url":     f"https://open.spotify.com/track/{fake_id}",
        "region":  str(region),
        "chart":   str(chart),
        "trend":   "SAME_POSITION",
        "streams": str(int(streams)) if streams else "",
    }


def _kafka_send(rows: list, description: str) -> html.Div:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    try:
        from kafka import KafkaProducer  # noqa: PLC0415

        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5_000,
            api_version_auto_timeout_ms=5_000,
        )
        for row in rows:
            producer.send("charts-feed", value=row)
        producer.flush(timeout=5)
        producer.close()

        return _feedback(
            f"Published {description} to Kafka. "
            "Appears in the live feed below in ~40 s.",
            ok=True,
        )
    except Exception as exc:
        return _feedback(
            f"Kafka unavailable ({exc}). "
            "Make sure the streaming stack is running: make streaming-up",
            ok=False,
        )


def _feedback(message: str, ok: bool) -> html.Div:
    return html.Div(message, style={
        "padding": "12px 16px",
        "borderRadius": "10px",
        "fontSize": "13px",
        "background": "rgba(76,175,80,0.12)" if ok else "rgba(255,107,107,0.12)",
        "border": f"1px solid {'#4CAF50' if ok else '#FF6B6B'}",
        "color": "#4CAF50" if ok else "#FF6B6B",
    })
