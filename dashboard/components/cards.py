"""Reusable card and layout components for the Resonance dashboard."""
from dash import dcc, html


def kpi_card(label: str, value: str, subtitle: str = "", accent: bool = False) -> html.Div:
    """Glass KPI card with large gradient number."""
    cls = "res-card kpi-card" + (" is-accent" if accent else "")
    return html.Div([
        html.Div(label, className="kpi-label"),
        html.Div(str(value), className="kpi-value"),
        html.Div(subtitle, className="kpi-sub") if subtitle else html.Span(),
    ], className=cls)


def section_header(title: str, meta: str = "", right=None) -> html.Div:
    """Section divider: bold title + optional meta text + optional right slot."""
    return html.Div([
        html.Div([
            html.H2(title),
            html.Span(meta, className="res-section-meta") if meta else html.Span(),
        ]),
        html.Div(right or [], className="res-section-right"),
    ], className="res-section-header")


def chart_card(title: str, content, meta: str = "", right=None) -> html.Div:
    """Glass chart card wrapper with header slot."""
    return html.Div([
        html.Div([
            html.Div([
                html.H4(title),
                html.Div(meta, className="res-chart-meta") if meta else html.Span(),
            ]),
            html.Div(right or []),
        ], className="res-chart-header"),
        content,
    ], className="res-chart")


def chip(text: str, accent: bool = False) -> html.Span:
    """Small pill badge."""
    cls = "res-chip" + (" accent" if accent else "")
    return html.Span(text, className=cls)
