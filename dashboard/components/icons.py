"""Monoline SVG icons (20×20 viewport, Apple SF-style strokes).

Uses base64-encoded data URIs with CSS mask-image so the icon inherits the
parent's CSS `color:` property (background: currentColor fills the mask shape).
"""
import base64
from dash import html

_PATHS = {
    "home":     "M3 9.5 10 4l7 5.5V16a1 1 0 0 1-1 1h-3v-5H8v5H4a1 1 0 0 1-1-1z",
    "pulse":    "M2 10h3l2-5 3 10 2-7 2 4h4",
    "flame":    "M10 17c3 0 5-2 5-5 0-3-3-5-3-8 0 0-2 2-2 5 0-1-1-2-2-2 0 2-3 3-3 6 0 2 2 4 5 4z",
    "timeline": "M3 5h14M3 10h14M3 15h14",
    "star":     "M10 3l2.4 4.8 5.3.8-3.8 3.7.9 5.3-4.8-2.5-4.8 2.5.9-5.3L2.3 8.6l5.3-.8z",
    "search":   "M9 3a6 6 0 1 1 0 12A6 6 0 0 1 9 3zm5 11 4 4",
    "filter":   "M3 5h14M6 10h8M9 15h2",
    "export":   "M10 3v11M5 9l5-6 5 6M4 17h12",
    "chevron":  "M7 5l6 5-6 5",
}


def icon(name: str, size: int = 16) -> html.Span:
    """SVG icon rendered via CSS mask-image (inherits CSS color property)."""
    d = _PATHS.get(name, _PATHS["star"])
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" '
        f'fill="none" stroke="black" stroke-width="1.6" '
        f'stroke-linecap="round" stroke-linejoin="round">'
        f'<path d="{d}"/></svg>'
    )
    b64 = base64.b64encode(svg.encode()).decode()
    url = f"data:image/svg+xml;base64,{b64}"
    return html.Span(
        style={
            "display": "inline-block",
            "width": f"{size}px",
            "height": f"{size}px",
            "flexShrink": "0",
            "background": "currentColor",
            "WebkitMaskImage": f"url('{url}')",
            "maskImage": f"url('{url}')",
            "WebkitMaskRepeat": "no-repeat",
            "maskRepeat": "no-repeat",
            "WebkitMaskSize": "contain",
            "maskSize": "contain",
        }
    )
