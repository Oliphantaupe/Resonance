"""Shared page-level shell components: topbar crystal + page hero."""
from dash import html


def topbar(crumb: str, controls=None) -> html.Div:
    """Floating crystal pill at the top of every page."""
    return html.Div([
        html.Div([
            html.Span("Resonance", className="res-muted"),
            html.Span("›", style={"opacity": "0.4", "margin": "0 4px"}),
            html.Strong(crumb),
        ], className="res-crumbs"),
        html.Div(controls or [], className="res-topbar-controls"),
    ], className="res-topcrystal")


def page_hero(eyebrow: str, title: str, subtitle: str = "", controls=None) -> html.Div:
    """Full-width hero area: eyebrow label + h1 + subtitle + right-aligned controls."""
    return html.Div([
        html.Div([
            html.Div(eyebrow, className="res-hero-eyebrow"),
            html.H1(title),
            html.P(subtitle) if subtitle else html.Span(),
        ], className="res-hero-text"),
        html.Div(controls or [], className="res-hero-controls"),
    ], className="res-hero")
