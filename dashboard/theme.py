"""Resonance — Plotly dark layout template.

Import this module to register the 'resonance' template and set it as
the global default. Any px.* or go.Figure() call made after this import
will automatically use the dark theme.

Usage:
    import dashboard.theme  # side-effect: sets pio.templates.default
"""
import plotly.io as pio

# ── Palette ──────────────────────────────────────────────────────────
CHART_COLORS = ["#7A6BFF", "#22D3EE", "#F472B6", "#FBBF24",
                "#34D399", "#FB7185", "#A78BFA", "#60A5FA"]

MOOD_COLORS = {
    "Happy/Energetic": "#F5C24A",
    "Calm/Peaceful":   "#5BE3C8",
    "Angry/Tense":     "#FF6B6B",
    "Sad/Depressing":  "#7AA8FF",
}

def hex_alpha(hex_color: str, alpha_hex: str) -> str:
    """Convert '#RRGGBB' + 2-digit alpha hex to 'rgba(r,g,b,a)' for Plotly."""
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    a = round(int(alpha_hex, 16) / 255, 3)
    return f"rgba({r},{g},{b},{a})"


BG       = "#0E0E11"
BG_PAGE  = "#0A0A0C"
TEXT     = "#FAFAFB"
TEXT_SEC = "#A6A6AE"
TEXT_TER = "#6B6B74"
ACCENT    = "#6E5BFF"
ACCENT_S  = "rgba(110,91,255,0.14)"
ACCENT_G  = "rgba(110,91,255,0.32)"
FONT     = ('"SF Pro Display","SF Pro Text",system-ui,-apple-system,'
            '"Segoe UI",Inter,Roboto,"Helvetica Neue",Arial,sans-serif')


def _axis(**kw):
    base = dict(
        gridcolor="rgba(255,255,255,0.04)",
        linecolor="rgba(255,255,255,0.07)",
        tickcolor="rgba(255,255,255,0.07)",
        tickfont=dict(color=TEXT_TER, size=11, family=FONT),
        titlefont=dict(color=TEXT_TER, size=12, family=FONT),
        zerolinecolor="rgba(255,255,255,0.08)",
        showgrid=True,
    )
    base.update(kw)
    return base


_LAYOUT = dict(
    paper_bgcolor=BG,
    plot_bgcolor=BG,
    font=dict(family=FONT, color=TEXT_SEC, size=12),
    title=dict(
        font=dict(color=TEXT, size=14, family=FONT),
        x=0.0, xanchor="left", pad=dict(l=4),
    ),
    colorway=CHART_COLORS,
    xaxis=_axis(),
    yaxis=_axis(),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        bordercolor="rgba(255,255,255,0.08)",
        borderwidth=1,
        font=dict(color=TEXT_SEC, size=12, family=FONT),
    ),
    hoverlabel=dict(
        bgcolor="#15151A",
        bordercolor="rgba(255,255,255,0.12)",
        font=dict(color=TEXT, size=12, family=FONT),
    ),
    polar=dict(
        bgcolor=BG,
        radialaxis=dict(
            gridcolor="rgba(255,255,255,0.06)",
            linecolor="rgba(255,255,255,0.07)",
            tickfont=dict(color=TEXT_TER, size=10),
        ),
        angularaxis=dict(
            linecolor="rgba(255,255,255,0.07)",
            tickfont=dict(color=TEXT_TER, size=11),
        ),
    ),
    geo=dict(
        bgcolor=BG,
        landcolor="#1A1A24",
        oceancolor=BG_PAGE,
        showocean=True,
        showlakes=False,
        showcountries=True,
        countrycolor="rgba(255,255,255,0.06)",
        coastlinecolor="rgba(255,255,255,0.08)",
        lakecolor=BG_PAGE,
        framecolor="rgba(255,255,255,0.06)",
        showframe=False,
    ),
    margin=dict(l=48, r=16, t=36, b=40),
    modebar=dict(bgcolor="rgba(0,0,0,0)", color=TEXT_TER, activecolor=TEXT),
)

# Build template by applying layout to a throwaway figure so all nested dicts
# get properly coerced into go.Layout sub-objects before extraction.
import plotly.graph_objects as go

_fig = go.Figure()
_fig.update_layout(**_LAYOUT)
pio.templates["resonance"] = go.layout.Template(layout=_fig.layout)
pio.templates.default = "resonance"
