"""
Plotting utilities for xminer using Plotly.

This module centralises a few reusable helpers that were previously
implemented directly inside the notebooks:

- Horizontal bar chart with party colouring (`plot_party_hbar`)
- Stacked vertical bar chart for tweet / engagement shares
  (`plot_party_stack_tweets_engagement`)
- Party pie chart based on percentage + absolute values
  (`plot_party_pie_pct`)

All functions return a Plotly Figure so you can either `.show()` them
inline in notebooks or further tweak them before saving.

Typical usage in a notebook:

    from pathlib import Path
    from xminer.utils.utils_plots import (
        plot_party_hbar,
        plot_party_stack_tweets_engagement,
        plot_party_pie_pct,
        STAND_TEXT,
        GRAPHICS_DIR,
    )

    STAND_TEXT = "Stand: 30.11.2025"
    GRAPHICS_DIR = Path("outputs/graphics")

    fig = plot_party_hbar(...)
    fig.show()

The implementations are intentionally lightweight and follow the
patterns from the existing notebooks – no over-engineering.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import yaml

# ---------------------------------------------------------------------------
# Global configuration
# ---------------------------------------------------------------------------


# --- Load parameters.yml (same file the pipelines use) ---
PARAMS_FILE = Path("C:/Users/felix/Documents/xminer/src/xminer/config/parameters.yml")
assert PARAMS_FILE.exists(), f"parameters.yml not found: {PARAMS_FILE}"

with PARAMS_FILE.open("r", encoding="utf-8") as f:
    params = yaml.safe_load(f) or {}

YEAR = int(params.get("year", 2025))
MONTH = int(params.get("month", 11))
YM = f"{YEAR:04d}{MONTH:02d}"

STAND_TEXT = f"Erhoben für {MONTH:02d}/{YEAR}"  # << das nutzt der Plot

GRAPHICS_BASE_DIR = Path(
    params.get(
        "graphics_base_dir",
        r"C:/Users/felix/Documents/xminer/outputs",
    )
)

GRAPHICS_DIR = GRAPHICS_BASE_DIR / YM / "graphics"
GRAPHICS_DIR.mkdir(parents=True, exist_ok=True)

STAND_TEXT = f"Erhoben für {MONTH:02d}/{YEAR}"

#: Default colours for parties, collected from the notebooks.
PARTY_COLORS: dict[str, str] = {
    "CDU/CSU": "#000000",
    "CDU": "#000000",
    "CSU": "#000000",
    "SPD": "#E3000F",
    "GRÜNE": "#1AA64A",
    "GRUENE": "#1AA64A",
    "BÜNDNIS 90/DIE GRÜNEN": "#1AA64A",
    "B90/GRUENE": "#1AA64A",
    "DIE LINKE.": "#BE3075",
    "DIE LINKE": "#BE3075",
    "LINKE": "#BE3075",
    "FDP": "#FFED00",
    "AFD": "#009EE0",
    "ALTERNATIVE FÜR DEUTSCHLAND": "#009EE0",
    "ALTERNATIVE FUER DEUTSCHLAND": "#009EE0",
    "BSW": "#009688",
    "FW": "#F28F00",
    "SSW": "#00A3E0",
    "PIRATEN": "#FF8800",
    "PARTEI": "#9E9E9E",
    "ÖDP": "#FF6A00",
    "OEDP": "#FF6A00",
}

#: Optional global subtitle text that is appended to figure titles.
#: This mirrors the pattern from the notebooks.

#: Optional directory for saving PNGs.  If you set this in your notebook
#: (e.g. `GRAPHICS_DIR = Path("outputs/graphics")`) you can just pass
#: `save_name="my_plot"` to the plotting functions.


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_party_value(p: str | None) -> str:
    """Normalise various party name spellings to a consistent key."""
    if p is None:
        return ""

    key = str(p).strip().upper()

    if key in {"CDU", "CSU"}:
        return "CDU/CSU"

    if (
        key.startswith("GRÜN")
        or key.startswith("GRUEN")
        or "GRUENE" in key
        or "GRÜNE" in key
        or "B90" in key
    ):
        return "GRÜNE"

    if key in {"LINKE", "DIE LINKE", "DIE LINKE."}:
        return "DIE LINKE."

    if key in {"ÖDP", "OEDP"}:
        return "ÖDP"

    if key in {"AFD", "ALTERNATIVE FÜR DEUTSCHLAND", "ALTERNATIVE FUER DEUTSCHLAND"}:
        return "AFD"

    return key


def _resolve_party_colors(parties: pd.Series | Iterable[str]) -> list[str]:
    """Map a series of party labels to colour hex strings."""
    if isinstance(parties, pd.Series):
        values = parties.astype("string").fillna("")
    else:
        values = pd.Series(list(parties), dtype="string").fillna("")

    return [PARTY_COLORS.get(_normalize_party_value(p), "#888888") for p in values]


def _is_dark_color(hex_color: str) -> bool:
    """Return True if colour is 'dark' based on simple brightness formula."""
    hex_color = hex_color.lstrip("#")
    if len(hex_color) != 6:
        return False
    r, g, b = (int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
    brightness = (r * 299 + g * 587 + b * 114) / 1000
    return brightness < 140


def _build_title(main_title: str | None) -> tuple[str | None, int]:
    """
    Combine main title with global STAND_TEXT and return (title_text, top_margin).
    Mirrors the logic used in the notebooks.
    """
    stand_text = STAND_TEXT
    if main_title and stand_text:
        return (
            f"{main_title}<br><sub style='font-size:0.85em; line-height:0.5;'>{stand_text}</sub>",
            100,
        )
    if main_title:
        return main_title, 50
    if stand_text:
        return stand_text, 60
    return None, 40


def _save_figure_if_requested(
    fig: go.Figure,
    save_name: str | None,
    *,
    width: int = 900,
    height: int = 600,
    scale: int = 2,
) -> None:
    """Save a Plotly figure to GRAPHICS_DIR as a PNG if `save_name` is given."""
    if not save_name:
        return

    if GRAPHICS_DIR is None:
        raise RuntimeError(
            "GRAPHICS_DIR not defined. Set xminer.utils.utils_plots.GRAPHICS_DIR before saving plots."
        )

    save_dir = Path(GRAPHICS_DIR)
    save_dir.mkdir(parents=True, exist_ok=True)
    save_path = save_dir / f"{save_name}.png"
    fig.write_image(save_path, width=width, height=height, scale=scale)
    print(f"✅ Plot saved to: {save_path}")


# ---------------------------------------------------------------------------
# Public plotting helpers
# ---------------------------------------------------------------------------


def plot_party_hbar(
    df_profiles: pd.DataFrame,
    y_col: str,
    x_col: str,
    top_n: int = 10,
    *,
    party_col: str = "partei_kurz",
    title: str | None = None,
    x_label: str | None = None,
    save_name: str | None = None,
) -> go.Figure:
    """
    Horizontal bar chart for profiles, coloured by party.

    Parameters
    ----------
    df_profiles:
        DataFrame containing at least ``FULLNAME``, ``y_col`` and ``x_col``.
    y_col:
        Column used inside the label (e.g. ``"username"``).
    x_col:
        Numeric column that defines bar length (e.g. counts, sums).
    top_n:
        Keep only the top N rows by ``x_col`` (descending).
    party_col:
        Column with party labels (used for colouring). If missing, bars
        fall back to a neutral grey.
    title:
        Optional title. If ``STAND_TEXT`` is set, it is appended in a smaller
        subtitle line – just like in the notebooks.
    x_label:
        Custom x axis label.  Defaults to ``x_col``.
    save_name:
        If provided, save a PNG to ``GRAPHICS_DIR / f"{save_name}.png"``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    required = {"FULLNAME", y_col, x_col}
    missing = required - set(df_profiles.columns)
    if missing:
        raise ValueError(f"Missing required columns in df_profiles: {missing}")

    work = df_profiles.copy()

    # Ensure party column exists
    if party_col not in work.columns:
        work[party_col] = None

    # Sort and cut to top N
    work = work.sort_values(x_col, ascending=False).head(top_n)

    # Combined label for y-axis: "Full Name (username)"
    work["_label"] = (
        work["FULLNAME"].astype(str).str.strip()
        + " ("
        + work[y_col].astype(str).str.strip()
        + ")"
    )

    # Category order: biggest at top (descending x), so we reverse for the axis
    categories = work["_label"].tolist()[::-1]
    work["_y_cat"] = pd.Categorical(work["_label"], categories=categories, ordered=True)

    # Resolve party colours
    colors = _resolve_party_colors(work[party_col])

    # Decide whether each bar is "short" or "long" to place text inside/outside
    max_x = float(work[x_col].max()) if len(work) else 0.0
    threshold = 0.15 * max_x if max_x > 0 else 0.0
    text_positions = ["outside" if float(x) < threshold else "inside" for x in work[x_col]]
    text_colors = [
        "#000000" if pos == "outside" else ("#FFFFFF" if _is_dark_color(c) else "#000000")
        for c, pos in zip(colors, text_positions)
    ]

    x_title = x_label or x_col

    fig = go.Figure(
        go.Bar(
            x=work[x_col],
            y=work["_y_cat"],
            orientation="h",
            marker_color=colors,
            text=[f"{v:,.0f}" for v in work[x_col]],
            textposition=text_positions,
            insidetextanchor="end",
            textfont=dict(color=text_colors),
            customdata=work[[party_col, y_col, "FULLNAME"]].astype(str).values,
            hovertemplate=(
                "Name: %{customdata[2]} (%{customdata[1]})<br>"
                f"{x_title}: %{{x:,.0f}}<br>"
                f"{party_col}: %{{customdata[0]}}<extra></extra>"
            ),
        )
    )

    title_text, top_margin = _build_title(title)

    fig.update_layout(
        title=dict(
            text=title_text,
            x=0.5,
            xanchor="center",
            yanchor="top",
            yref="container",
            font=dict(size=20),
        ),
        xaxis_title=x_title,
        yaxis_title="",
        yaxis=dict(categoryorder="array", categoryarray=categories),
        bargap=0.25,
        margin=dict(l=10, r=40, t=top_margin, b=10),
        height=max(300, 35 * len(work)),
        uniformtext_minsize=8,
        uniformtext_mode="show",
    )

    fig.update_traces(cliponaxis=False, texttemplate="%{text}")

    _save_figure_if_requested(fig, save_name)

    return fig


def plot_party_stack_tweets_engagement(
    df_party: pd.DataFrame,
    tweets_pct_col: str = "tweets_pct",
    engagement_pct_col: str = "engagement_sum_pct",
    *,
    party_col: str = "partei_kurz",
    title: str | None = None,
    save_name: str | None = None,
    min_inside_pct: float = 0.08,
) -> go.Figure:
    """
    Stacked vertical bar chart for per-party tweet / engagement shares.

    Generalised version of the notebook helper with the same name.

    Parameters
    ----------
    df_party:
        DataFrame with one row per party.
    tweets_pct_col:
        Column with tweet share (0–1).
    engagement_pct_col:
        Column with engagement share (0–1).
    party_col:
        Column with party labels (used for colouring).
    title:
        Optional title, combined with ``STAND_TEXT`` as subtitle.
    save_name:
        If provided, save a PNG to ``GRAPHICS_DIR / f"{save_name}.png"``.
    min_inside_pct:
        Threshold (0–1) from which the percentage text is put *inside*
        the bar segment instead of outside.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    for col in (tweets_pct_col, engagement_pct_col, party_col):
        if col not in df_party.columns:
            raise ValueError(f"Missing required column in df_party: {col}")

    work = df_party.copy()
    work[party_col] = work[party_col].astype(str).str.strip()

    x_vals = ["Anteil Tweets", "Anteil Impressions"]

    fig = go.Figure()

    for _, row in work.iterrows():
        party_label = row[party_col]
        key = _normalize_party_value(party_label)
        color = PARTY_COLORS.get(key, "#888888")

        y_vals = [row[tweets_pct_col], row[engagement_pct_col]]

        # Text as percentage
        text_vals = [f"{v * 100:.1f} %" if pd.notna(v) else "" for v in y_vals]

        text_positions: list[str] = []
        text_colors: list[str] = []
        for v in y_vals:
            if pd.isna(v):
                text_positions.append("outside")
                text_colors.append("#000000")
                continue

            if float(v) >= float(min_inside_pct):
                text_positions.append("inside")
                text_colors.append("#FFFFFF" if _is_dark_color(color) else "#000000")
            else:
                text_positions.append("outside")
                text_colors.append("#000000")

        fig.add_bar(
            name=key,
            x=x_vals,
            y=y_vals,
            marker_color=color,
            text=text_vals,
            textposition=text_positions,
            textfont=dict(color=text_colors, size=11),
            hovertemplate=(
                f"Partei: {key}<br>"
                "Kategorie: %{x}<br>"
                "Anteil: %{y:.1%}<extra></extra>"
            ),
        )

    title_text, top_margin = _build_title(title)

    fig.update_layout(
        title=dict(
            text=title_text,
            x=0.5,
            xanchor="center",
            yanchor="top",
            yref="container",
            font=dict(size=20),
        ),
        barmode="stack",
        xaxis_title="",
        yaxis_title="Anteil",
        yaxis=dict(tickformat=".0%"),
        margin=dict(l=40, r=40, t=top_margin, b=40),
        legend_title_text="Partei",
        uniformtext_minsize=8,
        uniformtext_mode="show",
    )

    _save_figure_if_requested(fig, save_name)

    return fig


def plot_party_pie_pct(
    df: pd.DataFrame,
    pct_col: str,
    sum_col: str,
    *,
    party_col: str = "partei_kurz",
    title: str = "Kumulierte Follower je Partei",
    save_name: str | None = None,
) -> go.Figure:
    """
    Pie chart of party shares.

    Parameters
    ----------
    df:
        DataFrame with one row per account / entity and party information.
    pct_col:
        Column with share values (0–1 fractions) that will be summed per party
        to determine slice sizes.
    sum_col:
        Column with absolute numbers (e.g. raw follower counts) that are also
        aggregated per party for the hover information.
    party_col:
        Column with party labels.
    title:
        Chart title (combined with ``STAND_TEXT`` as subtitle if set).
    save_name:
        If provided, save a PNG to ``GRAPHICS_DIR / f"{save_name}.png"``.

    Returns
    -------
    plotly.graph_objects.Figure
    """
    for col in (pct_col, sum_col, party_col):
        if col not in df.columns:
            raise ValueError(f"{col} not found in DataFrame")

    work = df.copy()
    work["party_norm"] = work[party_col].apply(_normalize_party_value)

    agg = (
        work.groupby("party_norm")
        .agg({pct_col: "sum", sum_col: "sum"})
        .sort_values(by=pct_col, ascending=False)
    )

    parties = agg.index.tolist()
    values = agg[pct_col].to_numpy()
    sums = agg[sum_col].to_numpy()
    colors = _resolve_party_colors(agg.index)

    title_text, _ = _build_title(title)

    fig = go.Figure(
        go.Pie(
            labels=parties,
            values=values,
            marker=dict(colors=colors),
            textinfo="label+percent",
            hovertemplate="%{label}<br>%{customdata[0]:.2%} Anteil<br>%{customdata[1]:,.0f} total<extra></extra>",
            customdata=np.stack([values, sums], axis=-1),
        )
    )

    fig.update_layout(
        title=dict(text=title_text, x=0.5),
        height=600,
        margin=dict(t=100, b=20, l=20, r=20),
    )

    _save_figure_if_requested(fig, save_name)

    return fig


# Backwards-compatible alias with a slightly shorter name
plot_party_stack_shares = plot_party_stack_tweets_engagement
