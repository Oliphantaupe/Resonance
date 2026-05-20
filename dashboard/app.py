"""Resonance — Dash app shell with floating dock navigation."""
import dash
import dash_bootstrap_components as dbc
from dash import Input, Output, html, dcc

import dashboard.theme  # registers 'resonance' Plotly template as default
from dashboard.components.icons import icon

# ── Navigation items ─────────────────────────────────────────────────
NAV = [
    {"path": "/",           "label": "Overview",     "glyph": "home"},
    {"path": "/sonic-dna",  "label": "Sonic DNA",    "glyph": "pulse"},
    {"path": "/streaks",    "label": "Streaks",      "glyph": "flame"},
    {"path": "/evolution",  "label": "Era Evolution","glyph": "timeline"},
    {"path": "/artists",    "label": "Top Artists",  "glyph": "star"},
    {"path": "/submit",     "label": "Submit",       "glyph": "export"},
]

# ── Dock ─────────────────────────────────────────────────────────────
def _dock_item(item: dict, idx: int) -> html.A:
    return html.A(
        [icon(item["glyph"], 18), html.Span(item["label"], className="res-dock-label")],
        href=item["path"],
        className="res-dock-item",
        id=f"dock-{idx}",
    )

dock = html.Div(
    html.Div(
        [_dock_item(item, i) for i, item in enumerate(NAV)],
        className="res-dock",
    ),
    className="res-dock-wrap",
)

# ── App ───────────────────────────────────────────────────────────────
app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Resonance",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

app.layout = html.Div([
    dcc.Location(id="_url"),
    html.Div(
        dash.page_container,
        className="res-page",
    ),
    dock,
], className="res-shell")


# ── Active dock state (server callback) ──────────────────────────────
@app.callback(
    [Output(f"dock-{i}", "className") for i in range(len(NAV))],
    Input("_url", "pathname"),
)
def _update_dock(pathname):
    classes = []
    for item in NAV:
        exact = item["path"] == "/"
        active = (pathname == "/") if exact else (pathname or "").startswith(item["path"])
        classes.append("res-dock-item active" if active else "res-dock-item")
    return classes


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8050, debug=False)
