# src/xminer/tasks/generate_trends_viz.py
"""
Generate visualizations for top Twitter/X trends.

Fetches current trends and creates visualizations showing how different political
parties are discussing trending topics. Similar to keyword_analysis notebook but
automated for daily/scheduled runs.

Usage:
    python -m xminer.tasks.generate_trends_viz [--limit N] [--dry-run] [--trend TREND_NAME]

Examples:
    # Generate visualizations for top 5 trends
    python -m xminer.tasks.generate_trends_viz

    # Limit to top 3 trends
    python -m xminer.tasks.generate_trends_viz --limit 3

    # Analyze a specific trend
    python -m xminer.tasks.generate_trends_viz --trend "#DigitalBlackoutIran"

    # Dry run (show what would be generated)
    python -m xminer.tasks.generate_trends_viz --dry-run
"""
import os
import re
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from sqlalchemy import text

from ..config.params import Params
from ..io.db import engine
from ..io.x_api_dual import client

try:
    from wordcloud import WordCloud
    WORDCLOUD_AVAILABLE = True
except ImportError:
    WORDCLOUD_AVAILABLE = False

# ---------- logging ----------
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/generate_trends_viz.log", mode="a"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# ---------- constants ----------
YEAR = int(Params.year)
MONTH = int(Params.month)
YM = f"{YEAR:04d}{MONTH:02d}"

# Politicians table - use latest available
POLITICIANS_YEAR = 2025
POLITICIANS_MONTH = 12
POLITICIANS_TABLE = f"politicians_{POLITICIANS_MONTH:02d}_{POLITICIANS_YEAR}"

# Output directory
GRAPHICS_BASE_DIR = Path(Params.graphics_base_dir) if Params.graphics_base_dir else Path("outputs")
GRAPHICS_DIR = GRAPHICS_BASE_DIR / YM / "graphics" / "trends"

# Party colors
PARTY_COLORS = {
    "CDU/CSU": "#000000",
    "CDU": "#000000",
    "CSU": "#000000",
    "SPD": "#E3000F",
    "GRÜNE": "#1AA64A",
    "BÜNDNIS 90/DIE GRÜNEN": "#1AA64A",
    "DIE LINKE.": "#BE3075",
    "LINKE": "#BE3075",
    "FDP": "#FFED00",
    "AFD": "#009EE0",
    "BSW": "#009688",
    "FW": "#F28F00",
    "SSW": "#00A3E0",
}

# Bilingual text
STAND_TEXT_DE = f"Erhoben für {MONTH:02d}/{YEAR}"
STAND_TEXT_EN = f"Data from {MONTH:02d}/{YEAR}"


def get_stand_text(language: str = 'de') -> str:
    return STAND_TEXT_DE if language == 'de' else STAND_TEXT_EN


def normalize_party(p: str) -> str:
    """Normalize party names for consistency."""
    if p is None:
        return ""
    key = str(p).strip().upper()
    if key in {"CDU", "CSU"}:
        return "CDU/CSU"
    if key.startswith("GRÜN") or "GRUENE" in key or "B90" in key or "BÜNDNIS" in key:
        return "GRÜNE"
    if key in {"LINKE", "DIE LINKE", "DIE LINKE."}:
        return "DIE LINKE."
    return key


def get_party_color(party: str) -> str:
    """Get color for a party."""
    normalized = normalize_party(party)
    return PARTY_COLORS.get(normalized, "#888888")


def fetch_current_trends(limit: int = 10) -> List[Dict[str, Any]]:
    """Fetch current trends from API."""
    woeid = int(Params.trends_woeid)
    trends = client.get_trends(woeid)
    return trends[:limit]


def analyze_trend_by_party_single(search_term: str) -> pd.DataFrame:
    """Query tweets mentioning a specific search term, grouped by party."""
    # Clean for SQL ILIKE
    clean_name = search_term.replace("'", "''")

    # Search both with and without hashtag
    query = text(f"""
        SELECT
            p.partei_kurz AS party,
            COUNT(*) AS tweet_count,
            COUNT(DISTINCT t.username) AS user_count,
            COALESCE(SUM(t.like_count), 0) AS total_likes,
            COALESCE(SUM(t.retweet_count), 0) AS total_retweets,
            COALESCE(SUM(t.impression_count), 0) AS total_impressions
        FROM public.tweets t
        JOIN public."{POLITICIANS_TABLE}" p ON LOWER(t.username) = LOWER(p.username)
        WHERE t.text ILIKE :pattern1 OR t.text ILIKE :pattern2
        GROUP BY p.partei_kurz
        ORDER BY tweet_count DESC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'pattern1': f'%{clean_name}%',
            'pattern2': f'%#{clean_name}%'
        })

    if df.empty:
        return df

    # Normalize party names
    df = df.copy()
    df.loc[:, 'party_norm'] = df['party'].apply(normalize_party)

    # Aggregate by normalized party
    df_agg = (
        df.groupby('party_norm')
        .agg({
            'tweet_count': 'sum',
            'user_count': 'sum',
            'total_likes': 'sum',
            'total_retweets': 'sum',
            'total_impressions': 'sum',
        })
        .reset_index()
        .sort_values('tweet_count', ascending=False)
    )

    return df_agg


def analyze_trend_by_party(trend_name: str) -> tuple[pd.DataFrame, str]:
    """Query tweets mentioning a trend, trying multiple search variants if needed.

    Returns:
        Tuple of (DataFrame with results, actual search term used)
        If no results found for any variant, returns (empty DataFrame, original trend name)
    """
    # Get search variants (original first, then extracted keywords)
    variants = get_search_variants(trend_name)

    logger.debug(f"Search variants for '{trend_name}': {variants}")

    for variant in variants:
        df = analyze_trend_by_party_single(variant)

        if not df.empty:
            if variant != trend_name.lstrip('#'):
                logger.info(f"  Using extracted keyword '{variant}' (original: '{trend_name}')")
            return df, variant

    # No results found for any variant
    return pd.DataFrame(), trend_name.lstrip('#')


def format_number(val: float) -> str:
    """Format large numbers with K/M suffix for readability."""
    if val >= 1_000_000:
        return f"{val/1_000_000:.1f}M"
    elif val >= 1_000:
        return f"{val/1_000:.0f}K"
    else:
        return f"{int(val)}"


def create_trend_bar_chart(
    df: pd.DataFrame,
    trend_name: str,
    metric: str = 'tweet_count',
    language: str = 'de',
    output_path: Optional[Path] = None
) -> None:
    """Create horizontal bar chart showing party engagement with a trend using matplotlib."""
    df_sorted = df.sort_values(metric, ascending=True).copy()

    colors = [get_party_color(party) for party in df_sorted['party_norm']]

    # Metric labels (bilingual)
    if language == 'de':
        metric_labels = {
            'tweet_count': 'Anzahl Tweets',
            'total_impressions': 'Impressionen',
            'total_likes': 'Likes',
            'user_count': 'Anzahl Politiker'
        }
        title = f"'{trend_name}' nach Partei"
        source_text = f"Quelle: X/Twitter • Stand: {MONTH:02d}/{YEAR}"
    else:
        metric_labels = {
            'tweet_count': 'Number of Tweets',
            'total_impressions': 'Impressions',
            'total_likes': 'Likes',
            'user_count': 'Number of Politicians'
        }
        title = f"'{trend_name}' by Party"
        source_text = f"Source: X/Twitter • Data: {MONTH:02d}/{YEAR}"

    metric_label = metric_labels.get(metric, metric)

    # Create figure with white background
    fig, ax = plt.subplots(figsize=(10, max(4, 0.8 * len(df_sorted))))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')

    # Create horizontal bars
    bars = ax.barh(
        df_sorted['party_norm'],
        df_sorted[metric],
        color=colors,
        edgecolor='white',
        linewidth=0.5,
        height=0.7
    )

    # Add value labels on bars
    max_val = df_sorted[metric].max()
    for bar, val in zip(bars, df_sorted[metric]):
        # Put label inside if bar is long enough, otherwise outside
        if val > max_val * 0.3:
            ax.text(
                bar.get_width() - max_val * 0.02,
                bar.get_y() + bar.get_height() / 2,
                format_number(val),
                va='center', ha='right',
                fontsize=12, fontweight='bold',
                color='white'
            )
        else:
            ax.text(
                bar.get_width() + max_val * 0.02,
                bar.get_y() + bar.get_height() / 2,
                format_number(val),
                va='center', ha='left',
                fontsize=12, fontweight='bold',
                color='#333333'
            )

    # Styling - larger, bolder text for visibility
    ax.set_xlabel(metric_label, fontsize=14, fontweight='bold', color='#1a1a1a')
    ax.set_title(title, fontsize=18, fontweight='bold', color='#1a1a1a', pad=15)

    # Clean up axes
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#888888')
    ax.spines['bottom'].set_color('#888888')

    # Format x-axis for large numbers - larger tick labels
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: format_number(x)))
    ax.tick_params(axis='both', colors='#1a1a1a', labelsize=13, width=1.5)
    ax.tick_params(axis='y', labelsize=14)  # Party names even larger

    # Light gridlines
    ax.xaxis.grid(True, linestyle='--', alpha=0.4, color='#888888')
    ax.set_axisbelow(True)

    # Add x-axis padding
    ax.set_xlim(0, max_val * 1.15)

    # Add source text at bottom
    fig.text(0.99, 0.02, source_text, ha='right', va='bottom',
             fontsize=10, color='#555555', style='italic')

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.12)

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        plt.close(fig)
    else:
        plt.close(fig)


def create_trends_heatmap(
    trends_data: List[Dict[str, Any]],
    language: str = 'de',
    output_path: Optional[Path] = None
) -> None:
    """Create heatmap showing trend usage intensity by party using matplotlib."""
    import numpy as np

    if language == 'de':
        title = "Trend-Heatmap nach Partei"
        xaxis_label = "Partei"
        yaxis_label = "Trend"
        source_text = f"Quelle: X/Twitter • Stand: {MONTH:02d}/{YEAR}"
    else:
        title = "Trend Heatmap by Party"
        xaxis_label = "Party"
        yaxis_label = "Trend"
        source_text = f"Source: X/Twitter • Data: {MONTH:02d}/{YEAR}"

    # Build data matrix
    trend_names = []
    all_parties = set()

    for td in trends_data:
        if td['df'] is not None and not td['df'].empty:
            trend_names.append(td['trend_name'])
            for party in td['df']['party_norm']:
                all_parties.add(party)

    if not trend_names or not all_parties:
        return

    # Define party order (most important first)
    party_order = ['CDU/CSU', 'SPD', 'GRÜNE', 'FDP', 'AFD', 'DIE LINKE.', 'BSW', 'FW', 'SSW']
    parties = [p for p in party_order if p in all_parties]
    parties += [p for p in all_parties if p not in parties]

    # Build data matrix
    data_matrix = []
    for td in trends_data:
        if td['df'] is not None and not td['df'].empty:
            party_counts = dict(zip(td['df']['party_norm'], td['df']['tweet_count']))
            row = [party_counts.get(party, 0) for party in parties]
            data_matrix.append(row)

    data_array = np.array(data_matrix)

    # Create figure
    fig, ax = plt.subplots(figsize=(max(8, len(parties) * 1.2), max(4, len(trend_names) * 0.8)))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')

    # Create heatmap
    im = ax.imshow(data_array, cmap='YlOrRd', aspect='auto')

    # Add colorbar
    cbar = ax.figure.colorbar(im, ax=ax, shrink=0.8)
    cbar.ax.set_ylabel('Tweets', rotation=-90, va="bottom", fontsize=13, fontweight='bold', color='#1a1a1a')
    cbar.ax.tick_params(colors='#1a1a1a', labelsize=12)

    # Set ticks - larger, bolder
    ax.set_xticks(range(len(parties)))
    ax.set_yticks(range(len(trend_names)))
    ax.set_xticklabels(parties, fontsize=13, fontweight='bold', color='#1a1a1a')
    ax.set_yticklabels(trend_names, fontsize=13, fontweight='bold', color='#1a1a1a')

    # Rotate x labels for better fit
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

    # Add value annotations
    for i in range(len(trend_names)):
        for j in range(len(parties)):
            val = data_array[i, j]
            if val > 0:
                # Use white text for dark cells, black for light cells
                text_color = 'white' if val > data_array.max() * 0.5 else '#1a1a1a'
                ax.text(j, i, format_number(val), ha="center", va="center",
                       color=text_color, fontsize=12, fontweight='bold')

    # Labels and title - larger, bolder
    ax.set_xlabel(xaxis_label, fontsize=14, fontweight='bold', color='#1a1a1a')
    ax.set_ylabel(yaxis_label, fontsize=14, fontweight='bold', color='#1a1a1a')
    ax.set_title(title, fontsize=18, fontweight='bold', color='#1a1a1a', pad=15)

    # Add source text
    fig.text(0.99, 0.02, source_text, ha='right', va='bottom',
             fontsize=10, color='#555555', style='italic')

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        plt.close(fig)
    else:
        plt.close(fig)


def create_trends_comparison_chart(
    trends_data: List[Dict[str, Any]],
    language: str = 'de',
    output_path: Optional[Path] = None
) -> None:
    """Create grouped bar chart comparing multiple trends by party using matplotlib."""
    import numpy as np

    if language == 'de':
        title = "Trend-Vergleich nach Partei"
        xaxis_label = "Partei"
        yaxis_label = "Anzahl Tweets"
        source_text = f"Quelle: X/Twitter • Stand: {MONTH:02d}/{YEAR}"
    else:
        title = "Trend Comparison by Party"
        xaxis_label = "Party"
        yaxis_label = "Number of Tweets"
        source_text = f"Source: X/Twitter • Data: {MONTH:02d}/{YEAR}"

    # Collect all parties with their total tweets
    party_totals = {}
    for td in trends_data:
        if td['df'] is not None and not td['df'].empty:
            for _, row in td['df'].iterrows():
                party = row['party_norm']
                party_totals[party] = party_totals.get(party, 0) + row['tweet_count']

    if not party_totals:
        return

    # Get top 6 parties by total tweets
    top_parties = sorted(party_totals.keys(), key=lambda p: party_totals[p], reverse=True)[:6]

    # Build data for each trend
    valid_trends = [td for td in trends_data if td['df'] is not None and not td['df'].empty]
    if not valid_trends:
        return

    trend_names = [td['trend_name'] for td in valid_trends]

    # Create figure
    fig, ax = plt.subplots(figsize=(max(10, len(top_parties) * 2), 6))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')

    x = np.arange(len(top_parties))
    width = 0.8 / len(valid_trends)  # Dynamic bar width

    # Color palette for trends
    trend_colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B', '#95C623']

    for i, td in enumerate(valid_trends):
        party_counts = dict(zip(td['df']['party_norm'], td['df']['tweet_count']))
        values = [party_counts.get(party, 0) for party in top_parties]
        offset = width * i - width * (len(valid_trends) - 1) / 2
        color = trend_colors[i % len(trend_colors)]

        bars = ax.bar(x + offset, values, width * 0.9, label=td['trend_name'],
                     color=color, edgecolor='white', linewidth=0.5)

        # Add value labels on top of bars
        for bar, val in zip(bars, values):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                       format_number(val), ha='center', va='bottom',
                       fontsize=10, fontweight='bold', color='#1a1a1a')

    # Styling - larger, bolder text
    ax.set_xlabel(xaxis_label, fontsize=14, fontweight='bold', color='#1a1a1a')
    ax.set_ylabel(yaxis_label, fontsize=14, fontweight='bold', color='#1a1a1a')
    ax.set_title(title, fontsize=18, fontweight='bold', color='#1a1a1a', pad=15)
    ax.set_xticks(x)
    ax.set_xticklabels(top_parties, fontsize=13, fontweight='bold', color='#1a1a1a')

    # Clean up axes
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#888888')
    ax.spines['bottom'].set_color('#888888')

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: format_number(x)))
    ax.tick_params(axis='both', colors='#1a1a1a', labelsize=12, width=1.5)

    # Light gridlines
    ax.yaxis.grid(True, linestyle='--', alpha=0.4, color='#888888')
    ax.set_axisbelow(True)

    # Legend - larger text
    ax.legend(loc='upper right', frameon=True, facecolor='white', edgecolor='#888888',
              fontsize=11, title='Trends', title_fontsize=12)

    # Add source text
    fig.text(0.99, 0.02, source_text, ha='right', va='bottom',
             fontsize=10, color='#555555', style='italic')

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.15)

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        plt.close(fig)
    else:
        plt.close(fig)


def create_trends_overview_chart(
    trends_data: List[Dict[str, Any]],
    language: str = 'de',
    output_path: Optional[Path] = None
) -> None:
    """Create stacked bar overview chart showing all trends by party using matplotlib."""
    if language == 'de':
        title = "Top Trends nach Partei"
        xaxis_label = "Anzahl Tweets"
        source_text = f"Quelle: X/Twitter • Stand: {MONTH:02d}/{YEAR}"
    else:
        title = "Top Trends by Party"
        xaxis_label = "Number of Tweets"
        source_text = f"Source: X/Twitter • Data: {MONTH:02d}/{YEAR}"

    # Collect data - build a proper matrix
    trend_names = []
    all_parties = set()

    for td in trends_data:
        if td['df'] is not None and not td['df'].empty:
            trend_names.append(td['trend_name'])
            for party in td['df']['party_norm']:
                all_parties.add(party)

    if not trend_names:
        return

    # Define party order (most important first)
    party_order = ['CDU/CSU', 'SPD', 'GRÜNE', 'FDP', 'AFD', 'DIE LINKE.', 'BSW', 'FW', 'SSW']
    parties = [p for p in party_order if p in all_parties]
    parties += [p for p in all_parties if p not in parties]

    # Build data matrix
    data = {party: [] for party in parties}
    for td in trends_data:
        if td['df'] is not None and not td['df'].empty:
            party_counts = dict(zip(td['df']['party_norm'], td['df']['tweet_count']))
            for party in parties:
                data[party].append(party_counts.get(party, 0))

    # Create figure
    fig, ax = plt.subplots(figsize=(10, max(4, 0.6 * len(trend_names))))
    fig.patch.set_facecolor('white')
    ax.set_facecolor('white')

    # Create stacked horizontal bars
    y_pos = range(len(trend_names))
    left = [0] * len(trend_names)

    for party in parties:
        values = data[party]
        color = get_party_color(party)
        ax.barh(y_pos, values, left=left, label=party, color=color,
                edgecolor='white', linewidth=0.5, height=0.7)
        left = [l + v for l, v in zip(left, values)]

    # Styling - larger, bolder text
    ax.set_yticks(y_pos)
    ax.set_yticklabels(trend_names, fontsize=13, fontweight='bold', color='#1a1a1a')
    ax.set_xlabel(xaxis_label, fontsize=14, fontweight='bold', color='#1a1a1a')
    ax.set_title(title, fontsize=18, fontweight='bold', color='#1a1a1a', pad=15)

    # Clean up axes
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color('#888888')
    ax.spines['bottom'].set_color('#888888')

    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: format_number(x)))
    ax.tick_params(axis='both', colors='#1a1a1a', labelsize=12, width=1.5)

    # Light gridlines
    ax.xaxis.grid(True, linestyle='--', alpha=0.4, color='#888888')
    ax.set_axisbelow(True)

    # Legend below the chart - larger text
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.15),
              ncol=min(len(parties), 5), frameon=False, fontsize=11)

    # Add source text
    fig.text(0.99, 0.02, source_text, ha='right', va='bottom',
             fontsize=10, color='#555555', style='italic')

    plt.tight_layout()
    plt.subplots_adjust(bottom=0.25)

    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        plt.close(fig)
    else:
        plt.close(fig)


# ---------- word cloud ----------
# German stopwords for word cloud filtering
GERMAN_STOPWORDS = {
    # Articles
    'der', 'die', 'das', 'den', 'dem', 'des', 'ein', 'eine', 'einer', 'einem', 'einen', 'eines',
    # Conjunctions
    'und', 'oder', 'aber', 'doch', 'wenn', 'weil', 'dass', 'ob', 'als', 'wie', 'so', 'auch',
    'denn', 'also', 'damit', 'daher', 'deshalb', 'trotzdem', 'jedoch', 'sondern', 'sonst',
    'weder', 'noch', 'sowohl', 'obwohl', 'falls', 'sofern', 'sobald', 'solange', 'nachdem',
    'bevor', 'ehe', 'bis', 'seit', 'während', 'indem', 'wobei', 'weshalb', 'weswegen',
    # Verbs (common)
    'ist', 'sind', 'war', 'waren', 'wird', 'werden', 'wurde', 'wurden', 'hat', 'haben', 'hatte', 'hatten',
    'sein', 'bin', 'bist', 'seid', 'gewesen', 'wäre', 'wären', 'sei', 'seien',
    'habe', 'hast', 'habt', 'gehabt', 'hätte', 'hätten',
    'werde', 'wirst', 'werdet', 'geworden', 'würde', 'würden',
    'können', 'kann', 'konnte', 'konnten', 'könnte', 'könnten', 'gekonnt',
    'müssen', 'muss', 'musste', 'mussten', 'müsste', 'müssten', 'gemusst',
    'sollen', 'soll', 'sollte', 'sollten', 'gesollt',
    'wollen', 'will', 'wollte', 'wollten', 'gewollt',
    'dürfen', 'darf', 'durfte', 'durften', 'dürfte', 'dürften', 'gedurft',
    'mögen', 'mag', 'mochte', 'mochten', 'möchte', 'möchten', 'gemocht',
    'gibt', 'geben', 'gab', 'gaben', 'gegeben', 'gäbe', 'gäben',
    'geht', 'gehen', 'ging', 'gingen', 'gegangen', 'ginge',
    'kommt', 'kommen', 'kam', 'kamen', 'gekommen', 'käme', 'kämen',
    'macht', 'machen', 'machte', 'machten', 'gemacht',
    'sagt', 'sagen', 'sagte', 'sagten', 'gesagt',
    'lässt', 'lassen', 'ließ', 'ließen', 'gelassen',
    'bleibt', 'bleiben', 'blieb', 'blieben', 'geblieben',
    'steht', 'stehen', 'stand', 'standen', 'gestanden',
    'nimmt', 'nehmen', 'nahm', 'nahmen', 'genommen',
    'findet', 'finden', 'fand', 'fanden', 'gefunden',
    'weiß', 'wissen', 'wusste', 'wussten', 'gewusst',
    'sieht', 'sehen', 'sah', 'sahen', 'gesehen',
    'heißt', 'heißen', 'hieß', 'hießen', 'geheißen',
    'braucht', 'brauchen', 'brauchte', 'brauchten', 'gebraucht',
    # Pronouns
    'ich', 'du', 'er', 'sie', 'es', 'wir', 'ihr', 'Sie',
    'mich', 'dich', 'ihn', 'uns', 'euch', 'sich',
    'mir', 'dir', 'ihm', 'ihnen', 'Ihnen',
    'mein', 'meine', 'meiner', 'meinem', 'meinen', 'meines',
    'dein', 'deine', 'deiner', 'deinem', 'deinen', 'deines',
    'seine', 'seiner', 'seinem', 'seinen', 'seines',
    'ihre', 'ihrer', 'ihrem', 'ihren', 'ihres',
    'unser', 'unsere', 'unserer', 'unserem', 'unseren', 'unseres',
    'euer', 'eure', 'eurer', 'eurem', 'euren', 'eures',
    'man', 'selbst', 'selber', 'einander',
    # Demonstratives/Relatives
    'dieser', 'diese', 'dieses', 'diesem', 'diesen',
    'jener', 'jene', 'jenes', 'jenem', 'jenen',
    'jeder', 'jede', 'jedes', 'jedem', 'jeden',
    'welcher', 'welche', 'welches', 'welchem', 'welchen',
    'solcher', 'solche', 'solches', 'solchem', 'solchen',
    # Prepositions
    'bei', 'mit', 'nach', 'von', 'vor', 'zu', 'zum', 'zur', 'aus', 'auf', 'an', 'in', 'im', 'am', 'um', 'für',
    'über', 'unter', 'durch', 'gegen', 'ohne', 'wegen', 'trotz', 'statt', 'anstatt',
    'außer', 'hinter', 'neben', 'zwischen',
    # Adverbs
    'nicht', 'schon', 'nur', 'sehr', 'mehr', 'viel', 'wenig', 'ganz', 'gar', 'fast', 'kaum',
    'hier', 'dort', 'da', 'dann', 'wann', 'wo', 'wohin', 'woher', 'hin', 'her',
    'jetzt', 'heute', 'morgen', 'gestern', 'immer', 'nie', 'niemals', 'oft', 'mal', 'wieder',
    'nun', 'bereits', 'bald', 'eben', 'gerade', 'gleich', 'sofort', 'endlich', 'zuerst', 'zuletzt',
    'etwa', 'ungefähr', 'circa', 'rund', 'ziemlich', 'genug', 'besonders', 'sogar', 'wohl',
    'eher', 'etwas', 'meist', 'meistens', 'manchmal', 'selten', 'stets', 'überhaupt',
    # Interrogatives
    'was', 'wer', 'wen', 'wem', 'wessen', 'warum', 'wieso', 'weshalb', 'weswegen', 'wieviel', 'wieviele',
    # Negation/Indefinites
    'kein', 'keine', 'keiner', 'keinem', 'keinen', 'keines', 'nichts', 'niemand',
    'alle', 'alles', 'allem', 'allen', 'aller',
    'andere', 'anderer', 'anderen', 'anderem', 'anderes', 'anders',
    'einige', 'einiger', 'einigen', 'einigem', 'einiges',
    'manche', 'mancher', 'manchen', 'manchem', 'manches',
    'mehrere', 'mehrerer', 'mehreren', 'mehrerem',
    'viele', 'vieler', 'vielen', 'vielem', 'vieles',
    'wenige', 'weniger', 'wenigen', 'wenigem', 'weniges',
    'beide', 'beider', 'beiden', 'beidem', 'beides',
    # Numbers
    'eins', 'zwei', 'drei', 'vier', 'fünf', 'sechs', 'sieben', 'acht', 'neun', 'zehn',
    'erste', 'ersten', 'erster', 'erstes', 'zweite', 'zweiten', 'zweiter', 'dritten',
    # Common filler words
    'ja', 'nein', 'halt', 'schließlich', 'eigentlich', 'letztlich',
    'natürlich', 'tatsächlich', 'wirklich', 'wahrscheinlich', 'möglicherweise', 'vermutlich',
    'vielleicht', 'bestimmt', 'sicher', 'sicherlich', 'gewiss', 'offenbar', 'anscheinend',
    # Twitter-specific
    'rt', 'https', 'http', 'co', 'amp', 'via', 'twitter', 'tweet', 'tweets',
    # English common words
    'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'had', 'her', 'was', 'one', 'our',
    'out', 'day', 'get', 'has', 'him', 'his', 'how', 'its', 'may', 'new', 'now', 'old', 'see', 'way',
    'who', 'did', 'own', 'say', 'she', 'too', 'use', 'with', 'this', 'that', 'from', 'have',
    'will', 'your', 'more', 'when', 'what', 'been', 'some', 'them', 'than', 'only', 'come', 'over',
    'such', 'into', 'year', 'just', 'know', 'take', 'people', 'good', 'could',
    'be', 'to', 'of', 'in', 'it', 'is', 'on', 'at', 'as', 'by', 'we', 'or', 'an', 'no', 'if',
}


def clean_text_for_wordcloud(text_content: str) -> str:
    """Clean tweet text for word cloud generation."""
    if not text_content:
        return ""
    # Remove URLs
    text_content = re.sub(r'https?://\S+', '', text_content)
    # Remove mentions
    text_content = re.sub(r'@\w+', '', text_content)
    # Remove hashtag symbols but keep the word
    text_content = re.sub(r'#(\w+)', r'\1', text_content)
    # Remove special characters, keep letters and German umlauts
    text_content = re.sub(r'[^\w\sÄäÖöÜüß]', ' ', text_content)
    # Remove numbers
    text_content = re.sub(r'\d+', '', text_content)
    # Convert to lowercase
    text_content = text_content.lower()
    # Remove extra whitespace
    text_content = ' '.join(text_content.split())
    return text_content


def get_wordcloud_color_for_party(party: str) -> str:
    """Get word cloud color for a party - use actual party colors, white for CDU/CSU."""
    normalized = normalize_party(party)
    # CDU/CSU uses white for visibility on dark background
    if normalized == "CDU/CSU":
        return '#333333'  # Dark gray for white background
    return PARTY_COLORS.get(normalized, '#888888')


def get_tweets_for_trend_single(search_term: str) -> pd.DataFrame:
    """Query tweet texts for a specific search term, grouped by party."""
    clean_name = search_term.replace("'", "''")

    query = text(f"""
        SELECT
            t.text,
            p.partei_kurz AS party
        FROM public.tweets t
        JOIN public."{POLITICIANS_TABLE}" p ON LOWER(t.username) = LOWER(p.username)
        WHERE t.text ILIKE :pattern1 OR t.text ILIKE :pattern2
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={
            'pattern1': f'%{clean_name}%',
            'pattern2': f'%#{clean_name}%'
        })

    if not df.empty:
        df = df.copy()
        df.loc[:, 'party_norm'] = df['party'].apply(normalize_party)

    return df


def get_tweets_for_trend(trend_name: str, search_term_override: str = None) -> pd.DataFrame:
    """Query tweet texts for a trend, grouped by party.

    Args:
        trend_name: Original trend name
        search_term_override: If provided, use this search term instead of trying variants
    """
    if search_term_override:
        return get_tweets_for_trend_single(search_term_override)

    # Try variants
    variants = get_search_variants(trend_name)
    for variant in variants:
        df = get_tweets_for_trend_single(variant)
        if not df.empty:
            return df

    return pd.DataFrame()


def create_party_wordcloud(
    party_name: str,
    texts: List[str],
    trend_name: str
) -> Optional[Any]:
    """Create a word cloud for a specific party's tweets."""
    if not WORDCLOUD_AVAILABLE:
        return None

    # Combine and clean all texts
    combined_text = ' '.join([clean_text_for_wordcloud(t) for t in texts if t])

    # Build stopwords including trend name variants
    trend_clean = trend_name.lstrip('#').lower()
    all_stopwords = GERMAN_STOPWORDS | {trend_clean}

    # Filter out stopwords and short words
    words = combined_text.split()
    filtered_words = [w for w in words if w not in all_stopwords and len(w) > 2]
    filtered_text = ' '.join(filtered_words)

    if not filtered_text.strip():
        return None

    # Get the party color
    wc_color = get_wordcloud_color_for_party(party_name)

    def party_color_func(*args, **kwargs):
        return wc_color

    # Generate word cloud
    wc = WordCloud(
        width=800,
        height=400,
        background_color='white',
        color_func=party_color_func,
        max_words=50,
        min_font_size=10,
        max_font_size=100,
        relative_scaling=0.5,
        collocations=False,
    ).generate(filtered_text)

    return wc


def create_trend_wordclouds(
    trend_name: str,
    language: str = 'de',
    output_dir: Optional[Path] = None,
    min_tweets: int = 3,
    search_term: str = None
) -> int:
    """Create word clouds for a trend, one per party.

    Args:
        trend_name: Original trend name (used for display/filenames)
        language: 'de' or 'en'
        output_dir: Output directory
        min_tweets: Minimum tweets required per party
        search_term: Actual search term to use (if different from trend_name)
    """
    if not WORDCLOUD_AVAILABLE:
        logger.warning("wordcloud package not installed - skipping word clouds")
        return 0

    # Get tweets for this trend (use search_term if provided)
    df_tweets = get_tweets_for_trend(trend_name, search_term_override=search_term)
    if df_tweets.empty:
        return 0

    # Get parties with enough tweets
    party_counts = df_tweets['party_norm'].value_counts()
    parties_to_plot = party_counts[party_counts >= min_tweets].index.tolist()

    if not parties_to_plot:
        return 0

    generated = 0
    safe_name = sanitize_filename(trend_name)

    # Text labels
    if language == 'de':
        tweets_label = "Tweets"
        source_text = f"Quelle: X/Twitter • Stand: {MONTH:02d}/{YEAR}"
    else:
        tweets_label = "Tweets"
        source_text = f"Source: X/Twitter • Data: {MONTH:02d}/{YEAR}"

    # Create combined word cloud figure
    n_parties = len(parties_to_plot)
    n_cols = min(2, n_parties)
    n_rows = (n_parties + 1) // 2

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(16, 5 * n_rows), squeeze=False)
    fig.patch.set_facecolor('white')

    # Flatten axes (squeeze=False ensures it's always 2D)
    axes = axes.flatten()

    for idx, party in enumerate(parties_to_plot):
        ax = axes[idx]
        ax.set_facecolor('white')

        party_texts = df_tweets[df_tweets['party_norm'] == party]['text'].tolist()
        display_color = get_party_color(party)

        wc = create_party_wordcloud(party, party_texts, trend_name)

        if wc:
            ax.imshow(wc, interpolation='bilinear')
            ax.set_title(f"{party}\n({len(party_texts)} {tweets_label})",
                        color=display_color, fontsize=18, fontweight='bold', pad=10)
        else:
            no_words = "Nicht genug Wörter" if language == 'de' else "Not enough words"
            ax.text(0.5, 0.5, f"{no_words}\n{party}",
                   ha='center', va='center', color='#666666', fontsize=14)
            ax.set_title(f"{party}", color=display_color, fontsize=18, fontweight='bold')

        ax.axis('off')

    # Hide unused subplots
    for idx in range(n_parties, len(axes)):
        axes[idx].set_visible(False)

    # Main title
    if language == 'de':
        main_title = f"Word Clouds: '{trend_name}' nach Partei"
    else:
        main_title = f"Word Clouds: '{trend_name}' by Party"

    fig.suptitle(main_title, fontsize=20, fontweight='bold', color='#1a1a1a', y=1.02)

    # Source text
    fig.text(0.99, 0.01, source_text, ha='right', va='bottom',
             fontsize=9, color='#666666', style='italic')

    plt.tight_layout()

    if output_dir:
        path = output_dir / f"{safe_name}_wordclouds_{language}.png"
        fig.savefig(path, dpi=150, bbox_inches='tight',
                    facecolor='white', edgecolor='none')
        plt.close(fig)
        generated += 1
    else:
        plt.close(fig)

    return generated


# ---------- trend name normalization ----------
# Common hashtag prefixes/suffixes that don't add semantic meaning
CAMPAIGN_WORDS = {
    # English
    'blackout', 'challenge', 'gate', 'breaking', 'trending', 'viral', 'alert',
    'news', 'update', 'protest', 'support', 'stand', 'stop', 'save', 'free',
    'justice', 'truth', 'digital', 'online', 'world', 'global', 'national',
    'international', 'day', 'week', 'month', 'year', 'now', 'today',
    # German
    'protest', 'demo', 'streik', 'solidarität', 'freiheit', 'wahrheit',
    'aktuell', 'jetzt', 'heute', 'morgen', 'breaking', 'eilmeldung',
    'nie', 'wieder', 'kein', 'keine', 'gegen', 'mit', 'für',
}

# Known German football club abbreviations (3-letter codes used in hashtags)
# These are unlikely to appear in politician tweets, so we skip them
FOOTBALL_CLUB_CODES = {
    'sge', 'bvb', 'fcb', 'rbl', 'bmg', 's04', 'svw', 'tsg', 'vfb', 'fcn',
    'ksc', 'hsv', 'bsc', 'fck', 'vfl', 'scf', 'wob', 'koe', 'fca', 'fcsp',
    'f95', 'rwe', 'fcm', 'boc', 'dsc', 'svs', 'dvb', 'svh', 'fcz', 'sc',
}

# Minimum word length to consider as a keyword
MIN_KEYWORD_LENGTH = 3


def split_camelcase(name: str) -> List[str]:
    """Split camelCase/PascalCase into individual words.

    Examples:
        'DigitalBlackoutIran' -> ['Digital', 'Blackout', 'Iran']
        'SGEBVB' -> ['SGEBVB'] (all caps stays together)
        'iPhone14' -> ['i', 'Phone', '14']
    """
    # Handle all-caps (likely abbreviations) - keep as is
    if name.isupper() and len(name) <= 10:
        return [name]

    # Split on transitions: lowercase->uppercase, letter->number, number->letter
    # Also handle consecutive uppercase like 'HTTPServer' -> ['HTTP', 'Server']
    words = []
    current_word = ""

    for i, char in enumerate(name):
        if i == 0:
            current_word = char
            continue

        prev_char = name[i - 1]
        next_char = name[i + 1] if i + 1 < len(name) else None

        # Start new word on:
        # 1. lowercase -> uppercase (camelCase)
        # 2. letter -> digit or digit -> letter
        # 3. uppercase -> uppercase followed by lowercase (HTTP -> HTTP + Server)
        should_split = False

        if prev_char.islower() and char.isupper():
            should_split = True
        elif prev_char.isalpha() and char.isdigit():
            should_split = True
        elif prev_char.isdigit() and char.isalpha():
            should_split = True
        elif prev_char.isupper() and char.isupper() and next_char and next_char.islower():
            should_split = True

        if should_split and current_word:
            words.append(current_word)
            current_word = char
        else:
            current_word += char

    if current_word:
        words.append(current_word)

    return words


def is_football_hashtag(name: str) -> bool:
    """Check if a hashtag is likely a football match hashtag (e.g., SGEBVB, FCBBMG).

    These are typically 6 characters with two 3-letter club codes.
    """
    if len(name) != 6:
        return False

    name_lower = name.lower()
    first_half = name_lower[:3]
    second_half = name_lower[3:]

    return first_half in FOOTBALL_CLUB_CODES and second_half in FOOTBALL_CLUB_CODES


def extract_keywords_from_trend(trend_name: str) -> List[str]:
    """Extract meaningful keywords from a trend name.

    Examples:
        '#DigitalBlackoutIran' -> ['Iran']
        '#Schnee' -> ['Schnee']
        '#SGEBVB' -> [] (football match, no useful keywords)
        '#BauernProteste2024' -> ['Bauern', 'Proteste']
    """
    # Remove hashtag
    clean_name = trend_name.lstrip('#')

    # Check if it's a football match hashtag - these won't have politician tweets
    if is_football_hashtag(clean_name):
        logger.debug(f"Skipping football hashtag: {clean_name}")
        return []

    # Split on non-alphanumeric (handles hashtags like "Berlin_Demo")
    parts = re.split(r'[_\-\s]+', clean_name)

    keywords = []
    for part in parts:
        # Split camelCase
        words = split_camelcase(part)

        for word in words:
            word_lower = word.lower()

            # Skip if it's a campaign word
            if word_lower in CAMPAIGN_WORDS:
                continue

            # Skip if it's a football club code
            if word_lower in FOOTBALL_CLUB_CODES:
                continue

            # Skip if too short (unless it's an abbreviation like 'SPD', 'AFD')
            if len(word) < MIN_KEYWORD_LENGTH and not word.isupper():
                continue

            # Skip pure numbers
            if word.isdigit():
                continue

            # Skip years (2020-2030)
            try:
                if word.isdigit() and 2020 <= int(word) <= 2030:
                    continue
            except ValueError:
                pass

            keywords.append(word)

    return keywords


def get_search_variants(trend_name: str) -> List[str]:
    """Get a list of search variants to try, from most specific to most general.

    Examples:
        '#DigitalBlackoutIran' -> ['DigitalBlackoutIran', 'Iran']
        '#Schnee' -> ['Schnee']
        '#BauernProteste2024' -> ['BauernProteste2024', 'Bauern', 'Proteste']
    """
    variants = []

    # Always try the original trend name first (without hashtag)
    original = trend_name.lstrip('#')
    variants.append(original)

    # Extract keywords
    keywords = extract_keywords_from_trend(trend_name)

    # Add individual keywords as variants (longest first, as they're often most specific)
    keywords_sorted = sorted(keywords, key=len, reverse=True)
    for kw in keywords_sorted:
        if kw.lower() != original.lower() and kw not in variants:
            variants.append(kw)

    return variants


def sanitize_filename(name: str) -> str:
    """Convert trend name to safe filename."""
    # Remove hashtag, special chars
    clean = name.lstrip('#')
    clean = re.sub(r'[^\w\s-]', '', clean)
    clean = clean.replace(' ', '_').lower()
    return clean[:50]  # Limit length


def generate_visualizations(
    trends: List[Dict[str, Any]],
    output_dir: Path,
    dry_run: bool = False
) -> int:
    """Generate visualizations for given trends."""
    output_dir.mkdir(parents=True, exist_ok=True)

    trends_data = []
    generated_count = 0

    for trend in trends:
        trend_name = trend.get('trend_name')
        if not trend_name:
            continue

        logger.info(f"Analyzing trend: {trend_name}")

        # Log search variants being tried
        variants = get_search_variants(trend_name)
        if len(variants) > 1:
            logger.info(f"  Search variants: {variants}")

        # Analyze trend by party (with automatic fallback to extracted keywords)
        df, actual_search_term = analyze_trend_by_party(trend_name)

        if df.empty:
            logger.info(f"  No politician tweets found for '{trend_name}' (tried: {variants})")
            continue

        total_tweets = df['tweet_count'].sum()
        # Show which search term actually worked
        if actual_search_term != trend_name.lstrip('#'):
            logger.info(f"  Found {total_tweets} tweets from {len(df)} parties using keyword '{actual_search_term}'")
        else:
            logger.info(f"  Found {total_tweets} tweets from {len(df)} parties")

        # Store both original trend name (for display) and actual search term used
        trends_data.append({
            'trend_name': trend_name,
            'search_term': actual_search_term,
            'df': df,
            'meta_description': trend.get('meta_description'),
        })

        if dry_run:
            logger.info(f"  [DRY RUN] Would generate charts for '{trend_name}'")
            continue

        # Generate charts for this trend
        safe_name = sanitize_filename(trend_name)

        for lang in ['de', 'en']:
            # Tweet count bar chart (use original trend name for display)
            path_tweets = output_dir / f"{safe_name}_tweets_{lang}.png"
            create_trend_bar_chart(df, trend_name, 'tweet_count', lang, path_tweets)
            logger.info(f"  Saved: {path_tweets.name}")
            generated_count += 1

            # Impressions bar chart (if data available)
            if df['total_impressions'].sum() > 0:
                path_impressions = output_dir / f"{safe_name}_impressions_{lang}.png"
                create_trend_bar_chart(df, trend_name, 'total_impressions', lang, path_impressions)
                logger.info(f"  Saved: {path_impressions.name}")
                generated_count += 1

            # Word clouds for this trend (use actual search term that worked)
            wc_count = create_trend_wordclouds(trend_name, lang, output_dir, search_term=actual_search_term)
            if wc_count > 0:
                logger.info(f"  Saved: {safe_name}_wordclouds_{lang}.png")
                generated_count += wc_count

    # Generate multi-trend visualizations if we have multiple trends with data
    if len(trends_data) > 1 and not dry_run:
        for lang in ['de', 'en']:
            # Stacked overview chart
            path_overview = output_dir / f"trends_overview_{lang}.png"
            create_trends_overview_chart(trends_data, lang, path_overview)
            logger.info(f"Saved: {path_overview.name}")
            generated_count += 1

            # Heatmap showing trend intensity by party
            path_heatmap = output_dir / f"trends_heatmap_{lang}.png"
            create_trends_heatmap(trends_data, lang, path_heatmap)
            logger.info(f"Saved: {path_heatmap.name}")
            generated_count += 1

            # Grouped bar comparison chart
            path_comparison = output_dir / f"trends_comparison_{lang}.png"
            create_trends_comparison_chart(trends_data, lang, path_comparison)
            logger.info(f"Saved: {path_comparison.name}")
            generated_count += 1

    return generated_count


def main():
    parser = argparse.ArgumentParser(description="Generate visualizations for top trends")
    parser.add_argument("--limit", type=int, default=5, help="Number of trends to analyze (default: 5)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be generated without saving")
    parser.add_argument("--trend", type=str, help="Analyze a specific trend instead of fetching top trends")
    parser.add_argument("--output-dir", type=str, help="Custom output directory")
    args = parser.parse_args()

    output_dir = Path(args.output_dir) if args.output_dir else GRAPHICS_DIR

    logger.info("=" * 60)
    logger.info("GENERATE TRENDS VISUALIZATIONS")
    logger.info("=" * 60)
    logger.info(f"Output directory: {output_dir}")
    logger.info(f"Politicians table: {POLITICIANS_TABLE}")

    if args.dry_run:
        logger.info("DRY RUN - no files will be saved")

    # Get trends to analyze
    if args.trend:
        trends = [{'trend_name': args.trend, 'meta_description': None}]
        logger.info(f"Analyzing specific trend: {args.trend}")
    else:
        logger.info(f"Fetching top {args.limit} trends...")
        trends = fetch_current_trends(args.limit)
        logger.info(f"Fetched {len(trends)} trends:")
        for t in trends:
            logger.info(f"  #{t.get('rank', '?')}: {t.get('trend_name')} ({t.get('meta_description') or 'N/A'})")

    # Generate visualizations
    count = generate_visualizations(trends, output_dir, args.dry_run)

    logger.info("=" * 60)
    if args.dry_run:
        logger.info(f"DRY RUN COMPLETE - would generate visualizations for {len(trends)} trends")
    else:
        logger.info(f"COMPLETE - generated {count} visualizations")
    logger.info(f"Output directory: {output_dir}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
