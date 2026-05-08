"""Sonic DNA — audio feature profiles by stream-decile rank tier."""
import dash
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

from dashboard.components.cards import chart_card, chip, kpi_card, section_header
from dashboard.components.shell import page_hero, topbar
from dashboard.data_loader import load_gold
from dashboard.theme import ACCENT, CHART_COLORS, hex_alpha

dash.register_page(__name__, path="/sonic-dna", title="Sonic DNA")

FEATURES = ["danceability", "energy", "speechiness",
            "acousticness", "instrumentalness", "liveness", "valence"]
FEATURE_LABELS = [f.replace("_", " ").title() for f in FEATURES]

# ── Layout ────────────────────────────────────────────────────────────
layout = html.Div([
    topbar("Sonic DNA", controls=[
        dcc.Dropdown(id="dna-tier-a",
                     options=[{"label": f"Decile {i}{'  (Hits)' if i==1 else '  (Long tail)' if i==10 else ''}",
                               "value": i} for i in range(1, 11)],
                     value=1, clearable=False, style={"width": "160px", "fontSize": "13px"}),
        html.Span("vs", className="res-muted", style={"padding": "0 4px"}),
        dcc.Dropdown(id="dna-tier-b",
                     options=[{"label": f"Decile {i}", "value": i} for i in range(1, 11)],
                     value=5, clearable=False, style={"width": "130px", "fontSize": "13px"}),
    ]),
    html.Div([
        page_hero(
            eyebrow="Sonic DNA",
            title="The sound profile of a hit",
            subtitle="Six audio attributes averaged across each stream-decile tier — "
                     "decile 1 = highest-streamed tracks, 10 = long tail.",
        ),

        html.Div(id="dna-kpis", className="res-grid-3", style={"marginTop": "24px"}),

        section_header("Audio fingerprint comparison",
                       meta="Radar + bar — select two deciles in the header"),
        html.Div([
            chart_card("Attribute radar",
                       dcc.Graph(id="dna-radar", style={"height": "380px"})),
            chart_card("Mean values side-by-side",
                       dcc.Graph(id="dna-bar",   style={"height": "380px"})),
        ], className="res-grid-2"),

        section_header("Feature gradient across all deciles",
                       meta="Higher decile number = lower total streams"),
        html.Div([
            html.Div([
                dcc.Dropdown(
                    id="dna-feature",
                    options=[{"label": l, "value": f} for f, l in zip(FEATURES, FEATURE_LABELS)],
                    value="valence", clearable=False,
                    style={"width": "200px", "fontSize": "13px"},
                ),
            ], style={"marginBottom": "12px"}),
            chart_card("Feature across decile tiers",
                       dcc.Graph(id="dna-gradient", style={"height": "340px"})),
        ]),
    ], className="res-content"),
])


# ── Callbacks ─────────────────────────────────────────────────────────
@callback(Output("dna-kpis", "children"), Input("dna-tier-a", "value"))
def update_kpis(tier):
    df = load_gold("sonic_dna_by_rank_tier")
    d1  = df[df["stream_decile"] == 1].iloc[0]
    d10 = df[df["stream_decile"] == 10].iloc[0]
    row = df[df["stream_decile"] == tier].iloc[0]
    return [
        kpi_card("Avg Chart Days (Hits)",     f"{d1['avg_chart_days']:.0f}", "Decile 1", accent=True),
        kpi_card("Avg Chart Days (Long Tail)", f"{d10['avg_chart_days']:.0f}", "Decile 10"),
        kpi_card("Rank Δ 7d",
                 f"{row['avg_rank_delta_7d']:+.1f}" if row['avg_rank_delta_7d'] else "—",
                 f"Decile {tier} momentum"),
    ]


@callback(
    Output("dna-radar", "figure"),
    Output("dna-bar",   "figure"),
    Input("dna-tier-a", "value"),
    Input("dna-tier-b", "value"),
)
def update_comparison(ta, tb):
    df = load_gold("sonic_dna_by_rank_tier")
    ra = df[df["stream_decile"] == ta].iloc[0]
    rb = df[df["stream_decile"] == tb].iloc[0]

    vals_a = [ra[f"mean_{f}"] for f in FEATURES]
    vals_b = [rb[f"mean_{f}"] for f in FEATURES]

    # Radar
    fig_r = go.Figure()
    for name, vals, col in [(f"Decile {ta}", vals_a, CHART_COLORS[0]),
                             (f"Decile {tb}", vals_b, CHART_COLORS[2])]:
        fig_r.add_trace(go.Scatterpolar(
            r=vals + [vals[0]], theta=FEATURE_LABELS + [FEATURE_LABELS[0]],
            fill="toself", fillcolor=hex_alpha(col, "33"),
            line=dict(color=col, width=2.2), name=name,
            hovertemplate="<b>%{theta}</b>: %{r:.3f}<extra></extra>",
        ))
    fig_r.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        margin=dict(l=30, r=30, t=30, b=30),
        legend=dict(orientation="h", y=-0.08, x=0.5, xanchor="center"),
    )

    # Bar
    compare = pd.DataFrame({
        "feature": FEATURE_LABELS,
        f"Decile {ta}": vals_a,
        f"Decile {tb}": vals_b,
    }).melt(id_vars="feature", var_name="tier", value_name="value")

    import plotly.express as px
    fig_b = px.bar(
        compare, x="feature", y="value", color="tier",
        barmode="group",
        color_discrete_sequence=[CHART_COLORS[0], CHART_COLORS[2]],
        labels={"value": "Mean", "feature": ""},
    )
    fig_b.update_layout(
        margin=dict(l=40, r=8, t=8, b=48),
        legend=dict(orientation="h", y=1.0, x=1, xanchor="right", yanchor="bottom"),
    )
    return fig_r, fig_b


@callback(Output("dna-gradient", "figure"), Input("dna-feature", "value"))
def update_gradient(feature):
    df = load_gold("sonic_dna_by_rank_tier").sort_values("stream_decile")
    import plotly.express as px
    fig = px.bar(
        df,
        x="stream_decile", y=f"mean_{feature}",
        error_y=f"std_{feature}",
        color="stream_decile",
        color_continuous_scale=[[0, ACCENT], [1, "#22D3EE"]],
        labels={"stream_decile": "Decile (1 = Hits)", f"mean_{feature}": feature.replace("_", " ").title()},
    )
    fig.update_layout(
        margin=dict(l=48, r=8, t=8, b=36),
        coloraxis_showscale=False,
    )
    fig.update_traces(marker_line_width=0)
    return fig
