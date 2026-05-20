"""Top Artists — rolling popularity + sonic fingerprint + live streaming feed."""
import dash
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

from dashboard.components.cards import chart_card, chip, kpi_card, section_header
from dashboard.components.shell import page_hero, topbar
from dashboard.data_loader import GOLD, available_tables, load_gold
from dashboard.theme import ACCENT, ACCENT_S, CHART_COLORS, hex_alpha

dash.register_page(__name__, path="/artists", title="Top Artists")

AUDIO_DIMS = ["danceability", "energy", "valence", "acousticness", "speechiness"]
AUDIO_LABELS = [d.title() for d in AUDIO_DIMS]

# ── Layout ────────────────────────────────────────────────────────────
layout = html.Div([
    # Auto-refresh interval — updates the live streaming section every 30 s
    dcc.Interval(id="streaming-interval", interval=30_000, n_intervals=0),

    topbar("Top Artists", controls=[
        dcc.Dropdown(
            id="artist-year",
            options=[{"label": str(y), "value": y} for y in range(2017, 2022)],
            value=2020, clearable=False,
            style={"width": "110px", "fontSize": "13px"},
        ),
        dcc.Dropdown(
            id="artist-topn",
            options=[{"label": f"Top {n}", "value": n} for n in [10, 15, 20, 30]],
            value=10, clearable=False,
            style={"width": "110px", "fontSize": "13px"},
        ),
    ]),
    html.Div([
        page_hero(
            eyebrow="Top Artists",
            title="The most-streamed artists, by month",
            subtitle="Pick an artist in the dropdown to see their sonic fingerprint and stream trajectory.",
        ),

        section_header("Leaderboard", meta="Total streams in selected year"),
        html.Div([
            chart_card("Streams by artist",
                       dcc.Graph(id="artist-bar", style={"height": "480px"})),
            html.Div([
                html.Div(id="artist-detail-card", className="res-card",
                         style={"padding": "20px 22px"}),
                chart_card("Sonic fingerprint",
                           dcc.Graph(id="artist-radar", style={"height": "280px"}),
                           meta="6-axis attribute mix"),
            ], className="res-stack"),
        ], className="res-grid-2-asym"),

        section_header("Select artists to compare rolling streams"),
        html.Div([
            dcc.Dropdown(id="artist-select", multi=True, placeholder="Choose artists…",
                         style={"fontSize": "13px"}),
        ], style={"marginBottom": "12px"}),
        chart_card("Rolling 90-day streams",
                   dcc.Graph(id="artist-rolling", style={"height": "320px"}),
                   meta="Avg rolling 90-day streams per artist · by month"),

        # Live streaming section — populated by dcc.Interval callback
        html.Div(id="streaming-live-section"),
    ], className="res-content"),
])


# ── Callbacks ─────────────────────────────────────────────────────────
@callback(
    Output("artist-select", "options"),
    Input("artist-year", "value"),
)
def populate_options(year):
    df = load_gold("global_top_artists")
    df["month_start"] = pd.to_datetime(df["month_start"])
    top = (
        df[df["month_start"].dt.year == year]
        .groupby("artist")["monthly_streams"].sum()
        .nlargest(60).index.tolist()
    )
    return [{"label": a, "value": a} for a in top]


@callback(
    Output("artist-bar",         "figure"),
    Output("artist-detail-card", "children"),
    Output("artist-radar",       "figure"),
    Input("artist-year", "value"),
    Input("artist-topn", "value"),
)
def update_leaderboard(year, top_n):
    df = load_gold("global_top_artists")
    df["month_start"] = pd.to_datetime(df["month_start"])
    dy = df[df["month_start"].dt.year == year]

    agg = (
        dy.groupby("artist")["monthly_streams"].sum()
        .nlargest(top_n).reset_index()
        .sort_values("monthly_streams")
    )

    colors = [ACCENT if i == len(agg) - 1 else "rgba(110,91,255,0.18)"
              for i in range(len(agg))]

    fig_bar = go.Figure(go.Bar(
        x=agg["monthly_streams"],
        y=agg["artist"],
        orientation="h",
        marker=dict(color=colors, line=dict(width=0)),
        text=[f"{v/1e9:.1f}B" for v in agg["monthly_streams"]],
        textposition="outside",
        textfont=dict(color="var(--res-text-tertiary)", size=11),
        hovertemplate="<b>%{y}</b><br>%{x:,} streams<extra></extra>",
    ))
    fig_bar.update_layout(
        margin=dict(l=4, r=64, t=8, b=24),
        xaxis=dict(title="Total Streams"),
        yaxis=dict(automargin=True),
    )

    # Top artist detail card
    top_artist = agg.iloc[-1]["artist"]
    top_streams = agg.iloc[-1]["monthly_streams"]
    rank_row = dy[dy["artist"] == top_artist]
    detail = [
        html.Div([
            html.Div(top_artist[0],
                     style={"width": "48px", "height": "48px", "borderRadius": "14px",
                            "background": f"linear-gradient(135deg, {ACCENT}, {CHART_COLORS[1]})",
                            "display": "grid", "placeItems": "center",
                            "fontWeight": "600", "fontSize": "18px", "color": "#fff",
                            "boxShadow": f"0 8px 22px -10px {ACCENT}50", "flexShrink": "0"}),
            html.Div([
                html.Div(top_artist, style={"fontSize": "17px", "fontWeight": "600",
                                            "letterSpacing": "-0.01em",
                                            "color": "var(--res-text)"}),
                html.Div(f"#{top_n} → #1 · {top_streams/1e9:.1f}B streams",
                         className="res-muted"),
            ]),
        ], style={"display": "flex", "alignItems": "center", "gap": "12px",
                  "marginBottom": "14px"}),
        chip(f"Year {year}", accent=True),
    ]

    # Radar for top artist
    artist_audio = (
        dy[dy["artist"] == top_artist]
        [[f"avg_{d}" for d in AUDIO_DIMS]].mean()
    )
    vals = [float(artist_audio.get(f"avg_{d}", 0) or 0) for d in AUDIO_DIMS]
    fig_radar = go.Figure(go.Scatterpolar(
        r=vals + [vals[0]], theta=AUDIO_LABELS + [AUDIO_LABELS[0]],
        fill="toself", fillcolor=hex_alpha(ACCENT, "33"),
        line=dict(color=ACCENT, width=2.2),
        marker=dict(color=ACCENT, size=6),
        hovertemplate="<b>%{theta}</b>: %{r:.2f}<extra></extra>",
    ))
    fig_radar.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
        margin=dict(l=28, r=28, t=28, b=28),
        showlegend=False,
    )
    return fig_bar, detail, fig_radar


@callback(
    Output("streaming-live-section", "children"),
    Input("streaming-interval", "n_intervals"),
)
def update_live_feed(n_intervals):
    """Poll for the streaming Gold table and render a live top-artists bar chart.

    Returns an empty div when the streaming pipeline has not yet written data.
    Clears the data_loader cache so each tick reads the latest Delta snapshot.
    """
    if "top_artists_streaming" not in available_tables():
        return html.Div()

    load_gold.cache_clear()
    try:
        df = load_gold("top_artists_streaming")
    except Exception:
        return html.Div()

    if df.empty:
        return html.Div()

    df["month_start"] = pd.to_datetime(df["month_start"])
    top = (
        df.groupby("artist")["monthly_streams"]
        .sum()
        .nlargest(10)
        .reset_index()
        .sort_values("monthly_streams")
    )

    fig = go.Figure(go.Bar(
        x=top["monthly_streams"],
        y=top["artist"],
        orientation="h",
        marker=dict(color=ACCENT, opacity=0.85, line=dict(width=0)),
        text=[f"{v/1e6:.0f}M" for v in top["monthly_streams"]],
        textposition="outside",
        textfont=dict(color="var(--res-text-tertiary)", size=11),
        hovertemplate="<b>%{y}</b><br>%{x:,} streams<extra></extra>",
    ))
    fig.update_layout(
        margin=dict(l=4, r=64, t=8, b=24),
        xaxis=dict(title="Total Streams (streaming feed)"),
        yaxis=dict(automargin=True),
    )

    total_rows = int(df.shape[0])
    return html.Div([
        section_header(
            "Live Feed",
            meta=f"Streaming pipeline · {total_rows:,} records ingested · refreshes every 30 s",
        ),
        html.Div(
            chip("● STREAMING LIVE", accent=True),
            style={"marginBottom": "12px"},
        ),
        chart_card(
            "Top artists from streaming ingestion",
            dcc.Graph(figure=fig, style={"height": "340px"}),
            meta="Kafka → Spark Structured Streaming → Delta → Dash",
        ),
    ])


@callback(Output("artist-rolling", "figure"), Input("artist-select", "value"))
def update_rolling(artists):
    if not artists:
        fig = go.Figure()
        fig.update_layout(
            annotations=[dict(text="Select artists above to compare",
                              showarrow=False, font=dict(color="var(--res-text-tertiary)", size=14),
                              xref="paper", yref="paper", x=0.5, y=0.5)],
            margin=dict(l=48, r=8, t=8, b=36),
        )
        return fig

    df = load_gold("global_top_artists")
    df["month_start"] = pd.to_datetime(df["month_start"])
    sub = df[df["artist"].isin(artists)]

    fig = go.Figure()
    for i, artist in enumerate(artists):
        ad = sub[sub["artist"] == artist].sort_values("month_start")
        col = CHART_COLORS[i % len(CHART_COLORS)]
        fig.add_trace(go.Scatter(
            x=ad["month_start"], y=ad["avg_rolling_90d_streams"],
            mode="lines+markers", name=artist,
            line=dict(color=col, width=2.2, shape="spline"),
            marker=dict(color=col, size=5, line=dict(color="#0E0E11", width=1.5)),
            fill="tozeroy", fillcolor=hex_alpha(col, "12"),
            hovertemplate=f"<b>{artist}</b><br>%{{x|%b %Y}}: %{{y:,.0f}}<extra></extra>",
        ))
    fig.update_layout(
        margin=dict(l=56, r=8, t=8, b=36),
        yaxis_title="Avg rolling 90d streams",
        xaxis_title="Month",
        legend=dict(orientation="h", y=1.04, x=1, xanchor="right"),
    )
    return fig
