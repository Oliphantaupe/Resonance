"""Era Evolution — audio features and chart longevity by release decade."""
import dash
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

from dashboard.components.cards import chart_card, kpi_card, section_header
from dashboard.components.shell import page_hero, topbar
from dashboard.data_loader import load_gold
from dashboard.theme import CHART_COLORS, hex_alpha

dash.register_page(__name__, path="/evolution", title="Era Evolution")

FEATURES = [
    ("energy",         "Energy"),
    ("valence",        "Valence"),
    ("danceability",   "Danceability"),
    ("acousticness",   "Acousticness"),
]

# ── Layout ────────────────────────────────────────────────────────────
layout = html.Div([
    topbar("Era Evolution"),
    html.Div([
        page_hero(
            eyebrow="Era Evolution",
            title="Sound, decade by decade",
            subtitle="How the average hit has shifted across audio attributes since the 1980s.",
        ),

        # Segmented attribute control
        html.Div(
            [html.Button(label, id=f"era-seg-{fid}", n_clicks=0,
                         className="res-seg-btn" + (" active" if i == 0 else ""),
                         **{"data-feature": fid})
             for i, (fid, label) in enumerate(FEATURES)],
            className="res-seg-wrap", id="era-seg-wrap",
            style={"marginTop": "24px"},
        ),
        dcc.Store(id="era-active-feature", data="energy"),

        section_header("Attribute timeline", meta="Mean value per release decade"),
        chart_card("Feature over decades",
                   dcc.Graph(id="era-trend-line", style={"height": "380px"})),

        section_header("Lifespan vs. feature", meta="How decades differ in chart staying power"),
        html.Div([
            chart_card("Median chart lifespan by decade",
                       dcc.Graph(id="era-lifespan-bar", style={"height": "320px"})),
            chart_card("Scatter: lifespan vs. feature",
                       dcc.Graph(id="era-scatter",      style={"height": "320px"}),
                       meta="Sampled 4 000 tracks"),
        ], className="res-grid-2"),

        # Per-decade spark cards
        section_header("Decade summary cards"),
        html.Div(id="era-spark-cards", className="res-grid-4"),

    ], className="res-content"),
])


# ── Clientside: highlight active seg button ───────────────────────────
for _fid, _ in FEATURES:
    @callback(
        Output(f"era-seg-{_fid}", "className"),
        Input("era-active-feature", "data"),
        prevent_initial_call=False,
    )
    def _seg_class(active, fid=_fid):
        return "res-seg-btn active" if active == fid else "res-seg-btn"


@callback(
    Output("era-active-feature", "data"),
    [Input(f"era-seg-{fid}", "n_clicks") for fid, _ in FEATURES],
    prevent_initial_call=True,
)
def pick_feature(*clicks):
    from dash import ctx
    if not ctx.triggered_id:
        return "energy"
    return ctx.triggered_id.replace("era-seg-", "")


# ── Main callbacks ────────────────────────────────────────────────────
@callback(
    Output("era-trend-line",  "figure"),
    Output("era-lifespan-bar","figure"),
    Output("era-scatter",     "figure"),
    Output("era-spark-cards", "children"),
    Input("era-active-feature", "data"),
)
def update_charts(feature):
    df = load_gold("track_longevity")
    df = df[df[feature].notna() & df["chart_lifespan_days"].notna()]
    label = dict(FEATURES).get(feature, feature.title())

    decade_feat = (
        df.groupby("release_decade")[feature].mean().reset_index()
        .rename(columns={feature: "mean_val"}).sort_values("release_decade")
    )
    decade_life = (
        df.groupby("release_decade")["chart_lifespan_days"].median().reset_index()
        .rename(columns={"chart_lifespan_days": "median_life"}).sort_values("release_decade")
    )

    # Trend line
    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(
        x=decade_feat["release_decade"].astype(str) + "s",
        y=decade_feat["mean_val"],
        mode="lines+markers",
        line=dict(color=CHART_COLORS[0], width=2.4, shape="spline"),
        marker=dict(color=CHART_COLORS[0], size=7, line=dict(color="#0E0E11", width=2)),
        fill="tozeroy",
        fillcolor=hex_alpha(CHART_COLORS[0], "14"),
        hovertemplate="<b>%{x}</b>: %{y:.3f}<extra></extra>",
    ))
    fig_line.update_layout(
        margin=dict(l=48, r=8, t=8, b=36),
        yaxis_title=label,
        xaxis_title="Release decade",
    )

    # Lifespan bar
    fig_bar = px.bar(
        decade_life.dropna(),
        x=decade_life.dropna()["release_decade"].astype(str) + "s",
        y="median_life",
        color="median_life",
        color_continuous_scale=[[0, hex_alpha(CHART_COLORS[0], "55")], [1, CHART_COLORS[1]]],
        labels={"x": "Decade", "median_life": "Median days"},
    )
    fig_bar.update_layout(margin=dict(l=48, r=8, t=8, b=36), coloraxis_showscale=False)
    fig_bar.update_traces(marker_line_width=0)

    # Scatter
    sample = df.sample(min(4000, len(df)), random_state=42)
    fig_scat = px.scatter(
        sample, x=feature, y="chart_lifespan_days",
        color="release_decade",
        opacity=0.55,
        labels={feature: label, "chart_lifespan_days": "Lifespan (days)",
                "release_decade": "Decade"},
    )
    fig_scat.update_layout(margin=dict(l=48, r=8, t=8, b=36),
                           legend=dict(orientation="h", y=-0.25, x=0.5, xanchor="center"))
    fig_scat.update_traces(marker_line_width=0, selector=dict(mode="markers"))

    # Spark cards
    cards = []
    for i, (decade, dg) in enumerate(df.groupby("release_decade")):
        if not decade:
            continue
        avg_val  = dg[feature].mean()
        avg_life = dg["chart_lifespan_days"].median()
        n_tracks = dg["track_id"].nunique()
        col = CHART_COLORS[i % len(CHART_COLORS)]

        # Tiny sparkline
        monthly = dg.groupby("first_chart_date" if "first_chart_date" in dg else "release_decade")[feature].mean()
        spark_fig = go.Figure(go.Scatter(
            y=dg.sort_values("chart_lifespan_days")[feature].values[:30],
            mode="lines",
            line=dict(color=col, width=1.8, shape="spline"),
            fill="tozeroy", fillcolor=hex_alpha(col, "18"),
        ))
        spark_fig.update_layout(
            margin=dict(l=0, r=0, t=0, b=0), height=56,
            xaxis=dict(visible=False), yaxis=dict(visible=False),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            showlegend=False,
        )

        cards.append(html.Div([
            html.Div([
                html.Span(style={"width": "8px", "height": "8px", "borderRadius": "50%",
                                 "background": col, "flexShrink": "0"}),
                html.Span(f"{int(decade)}s", style={"fontSize": "13px", "fontWeight": "500"}),
            ], style={"display": "flex", "alignItems": "center", "gap": "8px"}),
            html.Div(f"{avg_val:.2f}",
                     style={"fontSize": "28px", "fontWeight": "600",
                            "letterSpacing": "-0.03em", "fontVariantNumeric": "tabular-nums",
                            "color": "var(--res-text)", "marginTop": "6px"}),
            html.Div(f"avg {label.lower()}", className="res-muted"),
            html.Div(f"{avg_life:.0f}d avg lifespan · {n_tracks:,} tracks",
                     className="res-muted", style={"fontSize": "11px", "marginTop": "2px"}),
            dcc.Graph(figure=spark_fig, config={"displayModeBar": False},
                      style={"marginTop": "8px", "marginLeft": "-6px", "marginRight": "-6px"}),
        ], className="res-card", style={"padding": "16px 18px 12px"}))
    return fig_line, fig_bar, fig_scat, cards
