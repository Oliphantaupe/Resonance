"""Home — Regional Mood Map + global KPIs."""
import dash
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

from dashboard.components.cards import chart_card, chip, kpi_card, section_header
from dashboard.components.shell import page_hero, topbar
from dashboard.data_loader import load_gold
from dashboard.theme import MOOD_COLORS

dash.register_page(__name__, path="/", title="Overview")

# ── Layout ────────────────────────────────────────────────────────────
layout = html.Div([
    topbar("Overview", controls=[
        dcc.Dropdown(
            id="home-year",
            options=[{"label": str(y), "value": y} for y in range(2017, 2022)],
            value=2020, clearable=False,
            style={"width": "110px", "fontSize": "13px"},
        ),
    ]),
    html.Div([
        page_hero(
            eyebrow="Global Overview",
            title="Where the world is listening",
            subtitle="Dominant mood quadrant by region and season — 2017–2021.",
        ),

        # KPI row
        html.Div(id="home-kpis", className="res-grid-4", style={"marginTop": "24px"}),

        # Map + mood sidebar
        section_header("Regional Mood Map", meta="Dominant mood per region · by season"),
        dcc.Store(id="home-active-season", data="Spring"),
        html.Div([
            html.Div([
                html.Div(
                    [html.Button(s, id=f"home-season-{s.lower()}", n_clicks=0,
                                 className="res-seg-btn" + (" active" if s == "Spring" else ""))
                     for s in ["Spring", "Summer", "Autumn", "Winter"]],
                    className="res-seg-wrap",
                    style={"marginBottom": "12px"},
                ),
                chart_card(
                    "Streaming mood heatmap",
                    dcc.Graph(id="home-choropleth", style={"height": "400px"}),
                    meta="Dominant mood by region",
                ),
            ]),
            html.Div([
                chart_card(
                    "Mood distribution",
                    dcc.Graph(id="home-mood-donut", style={"height": "200px"}),
                    meta="Share by streams",
                ),
                chart_card(
                    "Season trend",
                    dcc.Graph(id="home-season-line", style={"height": "200px"}),
                ),
            ], className="res-stack"),
        ], className="res-grid-2-asym"),

        # Mood tiles
        section_header("Mood quadrants", meta="Stream share by quadrant this year"),
        html.Div(id="home-mood-tiles", className="res-grid-4"),
    ], className="res-content"),
])


# ── Callbacks ─────────────────────────────────────────────────────────
@callback(Output("home-kpis", "children"), Input("home-year", "value"))
def update_kpis(year):
    df = load_gold("regional_mood_seasonal")
    dy = df[df["year"] == year]
    total   = dy["total_streams"].sum()
    top_reg = dy.groupby("region")["total_streams"].sum().idxmax()
    top_mood = dy.groupby("mood_quadrant")["total_streams"].sum().idxmax()
    n_reg   = dy["region"].nunique()
    return [
        kpi_card("Total Streams",   f"{total/1e9:.1f}B", f"Year {year}", accent=True),
        kpi_card("Top Region",      top_reg[:20],        "by streams"),
        kpi_card("Dominant Mood",   top_mood.split("/")[0], "globally"),
        kpi_card("Regions Tracked", str(n_reg),          "in dataset"),
    ]


SEASONS = ["Spring", "Summer", "Autumn", "Winter"]

for _s in SEASONS:
    @callback(
        Output(f"home-season-{_s.lower()}", "className"),
        Input("home-active-season", "data"),
        prevent_initial_call=False,
    )
    def _season_cls(active, s=_s):
        return "res-seg-btn active" if active == s else "res-seg-btn"


@callback(
    Output("home-active-season", "data"),
    [Input(f"home-season-{s.lower()}", "n_clicks") for s in SEASONS],
    prevent_initial_call=True,
)
def _pick_season(*_):
    from dash import ctx
    if not ctx.triggered_id:
        return "Spring"
    return ctx.triggered_id.replace("home-season-", "").title()


@callback(
    Output("home-choropleth", "figure"),
    Input("home-year", "value"),
    Input("home-active-season", "data"),
)
def update_choropleth(year, season):
    df = load_gold("regional_mood_seasonal")
    dy = df[(df["year"] == year) & (df["season"] == season)]
    dominant = (
        dy[dy["mood_quadrant"].isin(MOOD_COLORS)]
        .sort_values("mood_share", ascending=False)
        .drop_duplicates(subset=["region"])
    )
    fig = px.choropleth(
        dominant,
        locations="region",
        locationmode="country names",
        color="mood_quadrant",
        hover_name="region",
        hover_data={"mood_share": ":.1%", "total_streams": ":,", "mood_quadrant": False},
        color_discrete_map=MOOD_COLORS,
        category_orders={"mood_quadrant": list(MOOD_COLORS.keys())},
        labels={"mood_quadrant": "Mood"},
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=8),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(
            orientation="h", y=-0.02, x=0.5, xanchor="center",
            title_text="", font=dict(size=12),
            itemsizing="constant",
        ),
        geo=dict(
            showframe=False,
            showcoastlines=True,
            coastlinecolor="rgba(255,255,255,0.15)",
            showland=True,
            landcolor="#1A1A26",
            showocean=True,
            oceancolor="#0E0E11",
            showlakes=False,
            showcountries=True,
            countrycolor="rgba(255,255,255,0.08)",
            bgcolor="rgba(0,0,0,0)",
            projection_type="natural earth",
        ),
    )
    return fig


@callback(
    Output("home-mood-donut", "figure"),
    Output("home-season-line", "figure"),
    Output("home-mood-tiles", "children"),
    Input("home-year", "value"),
)
def update_secondary(year):
    df = load_gold("regional_mood_seasonal")
    dy = df[df["year"] == year]

    # Donut
    mood_agg = dy.groupby("mood_quadrant")["total_streams"].sum().reset_index()
    fig_donut = go.Figure(go.Pie(
        labels=mood_agg["mood_quadrant"],
        values=mood_agg["total_streams"],
        hole=0.65,
        marker=dict(
            colors=[MOOD_COLORS.get(m, "#666") for m in mood_agg["mood_quadrant"]],
            line=dict(color="#0E0E11", width=3),
        ),
        textinfo="none",
        hovertemplate="<b>%{label}</b><br>%{percent}<extra></extra>",
    ))
    fig_donut.update_layout(
        margin=dict(l=8, r=8, t=8, b=8),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
    )

    # Season line — explicit traces to guarantee exactly 4 lines
    season_order = ["Spring", "Summer", "Autumn", "Winter"]
    agg = (
        dy[dy["mood_quadrant"].isin(MOOD_COLORS)]
        .groupby(["season", "mood_quadrant"])["total_streams"].sum()
        .reset_index()
    )
    fig_line = go.Figure()
    for mood, color in MOOD_COLORS.items():
        row = agg[agg["mood_quadrant"] == mood].copy()
        row["_ord"] = row["season"].map({s: i for i, s in enumerate(season_order)})
        row = row.sort_values("_ord")
        fig_line.add_trace(go.Scatter(
            x=row["season"], y=row["total_streams"],
            mode="lines+markers", name=mood,
            line=dict(color=color, width=2, shape="spline"),
            marker=dict(color=color, size=6, line=dict(color="#0E0E11", width=1.5)),
            hovertemplate=f"<b>{mood}</b><br>%{{x}}: %{{y:,.0f}}<extra></extra>",
        ))
    fig_line.update_layout(
        margin=dict(l=40, r=8, t=8, b=60),
        xaxis=dict(categoryorder="array", categoryarray=season_order),
        yaxis_title="Streams",
        showlegend=True,
        legend=dict(orientation="h", y=-0.28, x=0.5, xanchor="center",
                    font=dict(size=11), title_text=""),
        paper_bgcolor="rgba(0,0,0,0)",
    )

    # Mood tiles
    total_s = dy["total_streams"].sum()
    tiles = []
    for mood, color in MOOD_COLORS.items():
        sub = dy[dy["mood_quadrant"] == mood]
        share = sub["total_streams"].sum() / total_s if total_s else 0
        n = sub["region"].nunique()
        tiles.append(html.Div([
            html.Div([
                html.Span(style={"width": "10px", "height": "10px", "borderRadius": "50%",
                                 "background": color, "boxShadow": f"0 0 14px {color}80",
                                 "flexShrink": "0"}),
                html.Span(mood, style={"fontSize": "13px", "fontWeight": "500"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "10px"}),
            html.Div(f"{share:.0%}", style={"fontSize": "36px", "fontWeight": "600",
                                            "letterSpacing": "-0.03em",
                                            "fontVariantNumeric": "tabular-nums",
                                            "color": "var(--res-text)"}),
            html.Div([
                html.Div(style={"height": "4px", "borderRadius": "999px",
                                "background": "rgba(255,255,255,0.06)", "overflow": "hidden",
                                "flex": "1"},
                         children=html.Div(style={"width": f"{share*100:.1f}%", "height": "100%",
                                                  "background": color, "borderRadius": "inherit"})),
                html.Span(f"{n} regions", style={"fontSize": "11px",
                                                  "color": "var(--res-text-tertiary)",
                                                  "whiteSpace": "nowrap"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "10px"}),
        ], className="res-card", style={"padding": "18px 20px", "display": "flex",
                                        "flexDirection": "column", "gap": "10px"}))
    return fig_donut, fig_line, tiles
