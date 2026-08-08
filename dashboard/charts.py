"""Inline-SVG chart primitives for the static dashboard.

No JavaScript, no chart library, no CDN. Every chart is SVG emitted as a string
and embedded directly in the page, so the published artefact is a single file
that renders offline and cannot break because a CDN changed.

── Design constraints these functions enforce ──────────────────────────────
Colours are CSS custom properties defined once in generate_static.CSS, from a
palette validated for colour-vision deficiency in both light and dark mode.
Three rules here are structural rather than stylistic:

1. NO DUAL-AXIS CHARTS, and no small multiples with independent scales. Two
   measures on one plot with two y-scales invent a correlation, because where
   the scales line up is arbitrary. Two stacked plots each scaled to their own
   peak tell the same lie with more whitespace. Series in different units are
   INDEXED to a common base and drawn on one axis - see `indexed_lines`.

2. ZERO BASELINE, always. A truncated axis turns a 10% spread into apparent
   volatility. This is the most common way a correct dataset produces a
   misleading chart.

3. ONE SERIES, ONE COLOUR. Bar length already encodes magnitude; shading bars
   darker-where-bigger burns the only free channel to repeat information the
   chart already shows.

Every chart also emits a <title> per mark (native browser tooltip) and is
paired in the page with a <details> table view, so no value is reachable only
by hovering.
"""

from __future__ import annotations

from collections.abc import Sequence
from html import escape


def _fmt(value: float, decimals: int = 0) -> str:
    return f"{value:,.{decimals}f}"


def _points(values: Sequence[float], x_of, y_of) -> str:
    return " ".join(f"{x_of(i):.1f},{y_of(v):.1f}" for i, v in enumerate(values))


def bar_h(
    rows: Sequence[tuple[str, float]],
    *,
    width: int = 460,
    row_height: int = 26,
    label_width: int = 150,
    decimals: int = 0,
    suffix: str = "",
    series: str = "1",
) -> str:
    """Horizontal bars. Categories on the left, value labels at the bar end.

    Value labels sit OUTSIDE the bar rather than inside it: an in-bar label is
    clipped the moment a bar is short, and a clipped number is worse than none.
    """
    if not rows:
        return "<p class='empty'>No data</p>"

    height = len(rows) * row_height + 28
    plot_w = width - label_width - 70
    peak = max(v for _, v in rows) or 1

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'class="chart" preserveAspectRatio="xMinYMin meet">'
    ]

    for i, (label, value) in enumerate(rows):
        y = i * row_height + 6
        bar_w = max((value / peak) * plot_w, 1.5)
        text = f"{_fmt(value, decimals)}{suffix}"
        parts.append(
            f'<g class="mark">'
            f"<title>{escape(label)}: {text}</title>"
            f'<text class="cat" x="{label_width - 8}" y="{y + 13}" '
            f'text-anchor="end">{escape(label)}</text>'
            # rx=4 rounds the data end; the bar is anchored to the baseline.
            f'<rect class="bar s{series}" x="{label_width}" y="{y + 3}" '
            f'width="{bar_w:.1f}" height="{row_height - 12}" rx="4"/>'
            f'<text class="val" x="{label_width + bar_w + 6:.1f}" y="{y + 13}">{text}</text>'
            f"</g>"
        )

    parts.append("</svg>")
    return "".join(parts)


def column(
    rows: Sequence[tuple[str, float]],
    *,
    width: int = 520,
    height: int = 170,
    decimals: int = 0,
    suffix: str = "",
    series: str = "1",
    label_every: int = 3,
) -> str:
    """Vertical columns against a zero baseline.

    The y-axis ALWAYS starts at zero. A truncated baseline makes a 10% spread
    fill the plot and reads as volatility that is not in the data - the single
    most common way a correct dataset produces a misleading chart.
    """
    if not rows:
        return "<p class='empty'>No data</p>"

    pad_l, pad_b, pad_t = 44, 22, 10
    plot_w = width - pad_l - 8
    plot_h = height - pad_b - pad_t
    peak = max(v for _, v in rows) or 1
    step = plot_w / len(rows)

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'class="chart" preserveAspectRatio="xMinYMin meet">'
    ]

    # Recessive hairline gridlines, solid (never dashed - dashing reads as
    # "threshold" when it is only a grid).
    for frac in (0, 0.5, 1.0):
        gy = pad_t + plot_h * (1 - frac)
        parts.append(
            f'<line class="grid" x1="{pad_l}" y1="{gy:.1f}" x2="{width - 8}" y2="{gy:.1f}"/>'
            f'<text class="axis" x="{pad_l - 6}" y="{gy + 3:.1f}" text-anchor="end">'
            f"{_fmt(peak * frac, decimals)}</text>"
        )

    for i, (label, value) in enumerate(rows):
        bar_h_px = (value / peak) * plot_h
        x = pad_l + i * step
        # 2px surface gap between adjacent fills, not a stroke around them.
        w = max(step - 2, 1)
        y = pad_t + plot_h - bar_h_px
        parts.append(
            f'<g class="mark"><title>{escape(label)}: '
            f"{_fmt(value, decimals)}{suffix}</title>"
            f'<rect class="bar s{series}" x="{x:.1f}" y="{y:.1f}" '
            f'width="{w:.1f}" height="{max(bar_h_px, 0.8):.1f}" rx="3"/></g>'
        )
        if i % label_every == 0:
            parts.append(
                f'<text class="axis" x="{x + w / 2:.1f}" y="{height - 6}" '
                f'text-anchor="middle">{escape(label)}</text>'
            )

    parts.append(
        f'<line class="baseline" x1="{pad_l}" y1="{pad_t + plot_h}" '
        f'x2="{width - 8}" y2="{pad_t + plot_h}"/></svg>'
    )
    return "".join(parts)


def indexed_lines(
    labels: Sequence[str],
    series: Sequence[tuple[str, Sequence[float], str]],
    *,
    width: int = 900,
    height: int = 240,
    label_every: int = 2,
) -> str:
    """Two or more series on ONE axis, each indexed to its own total.

    ── Why indexing rather than raw counts ────────────────────────────────
    Trips requested and driver sessions are different units, so no shared
    y-scale is meaningful and no second y-scale is honest: where two scales
    line up is an arbitrary choice that invents a correlation.

    Small multiples do not escape this either. Two stacked plots each scaled
    to their OWN peak make a 780-session hour look the same height as a
    2,075-trip hour, which is the same lie with more whitespace.

    Expressing both as "share of this series' own daily total" puts them in
    the same unit (%) on one axis. Level is no longer comparable - that is the
    point, it never was - but SHAPE and TIMING are, and the timing offset
    between when supply arrives and when demand peaks is the whole question.

    Values are pre-indexed by the caller; this only draws them.
    """
    if not labels or not series:
        return "<p class='empty'>No data</p>"

    pad_l, pad_b, pad_t = 40, 22, 16
    plot_w = width - pad_l - 14
    plot_h = height - pad_b - pad_t
    peak = max((max(vals) for _, vals, _ in series), default=1) or 1
    step = plot_w / max(len(labels) - 1, 1)

    def x_of(i: int) -> float:
        return pad_l + i * step

    def y_of(v: float) -> float:
        return pad_t + plot_h * (1 - v / peak)

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'class="chart" preserveAspectRatio="xMinYMin meet">'
    ]

    for frac in (0, 0.5, 1.0):
        gy = pad_t + plot_h * (1 - frac)
        parts.append(
            f'<line class="grid" x1="{pad_l}" y1="{gy:.1f}" x2="{width - 14}" y2="{gy:.1f}"/>'
            f'<text class="axis" x="{pad_l - 6}" y="{gy + 3:.1f}" text-anchor="end">'
            f"{_fmt(peak * frac, 1)}%</text>"
        )

    for name, vals, slot in series:
        parts.append(f'<polyline class="line s{slot}" points="{_points(vals, x_of, y_of)}"/>')
        # Direct-label the endpoint of each series, so identity never depends
        # on colour alone even though a legend is also present.
        parts.append(
            f'<text class="val lbl s{slot}" x="{x_of(len(vals) - 1) + 5:.1f}" '
            f'y="{y_of(vals[-1]) + 3:.1f}">{escape(name)}</text>'
        )

    for i, label in enumerate(labels):
        tip = " · ".join(f"{name} {vals[i]:.1f}%" for name, vals, _ in series if i < len(vals))
        parts.append(
            f'<rect class="hit" x="{x_of(i) - step / 2:.1f}" y="{pad_t}" '
            f'width="{max(step, 8):.1f}" height="{plot_h}">'
            f"<title>{escape(label)}:00 — {escape(tip)}</title></rect>"
        )
        if i % label_every == 0:
            parts.append(
                f'<text class="axis" x="{x_of(i):.1f}" y="{height - 6}" '
                f'text-anchor="middle">{escape(label)}</text>'
            )

    parts.append(
        f'<line class="baseline" x1="{pad_l}" y1="{pad_t + plot_h}" '
        f'x2="{width - 14}" y2="{pad_t + plot_h}"/></svg>'
    )
    return "".join(parts)


def line(
    rows: Sequence[tuple[str, float]],
    *,
    width: int = 520,
    height: int = 190,
    decimals: int = 1,
    suffix: str = "",
    series: str = "1",
    label_every: int = 3,
) -> str:
    """Single-series line, zero-based, with a marker on the peak only.

    Direct-labelling every point is chaos and goes unread; the extreme is
    labelled and the rest is carried by the axis and the hover tooltip.
    """
    if not rows:
        return "<p class='empty'>No data</p>"

    pad_l, pad_b, pad_t = 44, 22, 14
    plot_w = width - pad_l - 12
    plot_h = height - pad_b - pad_t
    values = [v for _, v in rows]
    peak = max(values) or 1
    step = plot_w / max(len(rows) - 1, 1)

    def x_of(i: int) -> float:
        return pad_l + i * step

    def y_of(v: float) -> float:
        return pad_t + plot_h * (1 - v / peak)

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" '
        f'class="chart" preserveAspectRatio="xMinYMin meet">'
    ]

    for frac in (0, 0.5, 1.0):
        gy = pad_t + plot_h * (1 - frac)
        parts.append(
            f'<line class="grid" x1="{pad_l}" y1="{gy:.1f}" x2="{width - 12}" y2="{gy:.1f}"/>'
            f'<text class="axis" x="{pad_l - 6}" y="{gy + 3:.1f}" text-anchor="end">'
            f"{_fmt(peak * frac, decimals)}</text>"
        )

    parts.append(f'<polyline class="line s{series}" points="{_points(values, x_of, y_of)}"/>')

    peak_i = values.index(peak)
    parts.append(
        f'<circle class="dot s{series}" cx="{x_of(peak_i):.1f}" '
        f'cy="{y_of(peak):.1f}" r="4.5"/>'
        f'<text class="val peak" x="{x_of(peak_i):.1f}" y="{y_of(peak) - 9:.1f}" '
        f'text-anchor="middle">{_fmt(peak, decimals)}{suffix}</text>'
    )

    # Invisible wide hit targets: a 2px line is an impossible hover target, and
    # the tooltip must not require landing on the stroke.
    for i, (label, value) in enumerate(rows):
        parts.append(
            f'<rect class="hit" x="{x_of(i) - step / 2:.1f}" y="{pad_t}" '
            f'width="{max(step, 8):.1f}" height="{plot_h}">'
            f"<title>{escape(label)}: {_fmt(value, decimals)}{suffix}</title></rect>"
        )
        if i % label_every == 0:
            parts.append(
                f'<text class="axis" x="{x_of(i):.1f}" y="{height - 6}" '
                f'text-anchor="middle">{escape(label)}</text>'
            )

    parts.append(
        f'<line class="baseline" x1="{pad_l}" y1="{pad_t + plot_h}" '
        f'x2="{width - 12}" y2="{pad_t + plot_h}"/></svg>'
    )
    return "".join(parts)


def table_view(headers: Sequence[str], rows: Sequence[Sequence[str]], *, label: str) -> str:
    """The WCAG-clean twin of a chart.

    Every chart on the page ships one. A tooltip enhances; it must never be the
    only route to a value.
    """
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{escape(str(c))}</td>" for c in r) + "</tr>" for r in rows
    )
    return (
        f"<details class='tableview'><summary>{escape(label)}</summary>"
        f"<div class='scroll'><table><thead><tr>{head}</tr></thead>"
        f"<tbody>{body}</tbody></table></div></details>"
    )
