"""Streaks — longest consecutive #1 runs per region."""
import dash
import plotly.express as px
from dash import Input, Output, callback, dash_table, dcc, html

from dashboard.components.cards import chart_card, chip, kpi_card, section_header
from dashboard.components.shell import page_hero, topbar
from dashboard.data_loader import load_gold
from dashboard.theme import ACCENT, ACCENT_S, CHART_COLORS

dash.register_page(__name__, path="/streaks", title="Streaks")

# ── Layout ────────────────────────────────────────────────────────────
layout = html.Div([
    topbar("Streaks", controls=[
        dcc.Dropdown(id="streak-region", value="Global", clearable=False,
                     style={"width": "180px", "fontSize": "13px"}),
        dcc.Dropdown(
            id="streak-topn",
            options=[{"label": f"Top {n}", "value": n} for n in [10, 20, 30, 50]],
            value=20, clearable=False,
            style={"width": "110px", "fontSize": "13px"},
        ),
    ]),
    html.Div([
        page_hero(
            eyebrow="Streak Champions",
            title="Tracks that refused to leave the top spot",
            subtitle="Longest consecutive days at #1 by region — 2017 to 2021.",
        ),

        html.Div(id="streak-kpis", className="res-grid-3", style={"marginTop": "24px"}),

        section_header("Streak length", meta="Click a bar to explore"),
        chart_card("Days at #1",
                   dcc.Graph(id="streak-bar"),
                   meta="Sorted by streak length"),

        section_header("Streak ledger",
                       meta="All streak records",
                       right=[html.Span("", id="streak-count-chip", className="res-chip")]),
        html.Div(id="streak-table-wrap", className="res-card res-table",
                 style={"overflow": "hidden"}),
    ], className="res-content"),
])


# ── Callbacks ─────────────────────────────────────────────────────────
@callback(Output("streak-region", "options"), Input("streak-region", "id"))
def populate_regions(_):
    df = load_gold("streak_analysis")
    regions = sorted(df["region"].dropna().unique())
    return [{"label": "Global (all)", "value": "Global"}] + [{"label": r, "value": r} for r in regions]


@callback(Output("streak-kpis", "children"), Input("streak-region", "value"))
def update_kpis(region):
    df = load_gold("streak_analysis")
    sub = df if region == "Global" else df[df["region"] == region]
    if sub.empty:
        return []
    longest = int(sub["streak_days"].max())
    top_idx  = sub["streak_days"].idxmax()
    top_title = sub.loc[top_idx, "title"][:28]
    avg = sub["streak_days"].mean()
    n   = len(sub)
    return [
        kpi_card("Longest Streak", f"{longest} days", top_title, accent=True),
        kpi_card("Total Streaks",  str(n),            f"in {region}"),
        kpi_card("Average Streak", f"{avg:.1f} days", "across all tracks"),
    ]


@callback(
    Output("streak-bar",         "figure"),
    Output("streak-table-wrap",  "children"),
    Output("streak-count-chip",  "children"),
    Input("streak-region",  "value"),
    Input("streak-topn",    "value"),
)
def update_charts(region, top_n):
    df = load_gold("streak_analysis")
    sub = df if region == "Global" else df[df["region"] == region]
    top = sub.nlargest(top_n, "streak_days").copy()
    top["label"] = top["title"].str[:32] + " · " + top["artist"].str[:18]

    max_days = top["streak_days"].max()

    fig = px.bar(
        top.sort_values("streak_days"),
        x="streak_days", y="label", orientation="h",
        color="streak_days",
        color_continuous_scale=[[0, ACCENT_S], [0.5, ACCENT], [1, "#FBBF24"]],
        labels={"streak_days": "Days at #1", "label": ""},
        custom_data=["title", "artist", "region"],
    )
    fig.update_traces(
        hovertemplate="<b>%{customdata[0]}</b><br>%{customdata[1]}<br>"
                      "%{customdata[2]} · %{x} days<extra></extra>",
        marker_line_width=0,
        texttemplate="%{x}",
        textposition="outside",
        textfont_color="var(--res-text-tertiary)",
        textfont_size=11,
    )
    fig.update_layout(
        height=max(380, top_n * 24),
        margin=dict(l=4, r=56, t=8, b=32),
        yaxis=dict(categoryorder="total ascending"),
        coloraxis_showscale=False,
    )

    # Table
    cols = ["title", "artist", "region", "streak_start", "streak_end", "streak_days"]
    table = dash_table.DataTable(
        data=top[cols].to_dict("records"),
        columns=[{"name": c.replace("_", " ").title(), "id": c} for c in cols],
        sort_action="native",
        page_size=15,
        style_table={"overflowX": "auto", "background": "transparent"},
        style_cell={"textAlign": "left", "padding": "11px 14px",
                    "background": "transparent", "border": "none",
                    "color": "var(--res-text)", "fontSize": "13px",
                    "fontFamily": "var(--res-font)"},
        style_header={"background": "rgba(255,255,255,0.03)",
                      "color": "var(--res-text-tertiary)",
                      "fontWeight": "600", "fontSize": "11px",
                      "letterSpacing": "0.08em", "textTransform": "uppercase",
                      "border": "none",
                      "borderBottom": "1px solid var(--res-border)"},
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "background": "rgba(255,255,255,0.015)"},
        ],
    )

    return fig, table, f"{len(sub):,} total records"
