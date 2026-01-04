"""
Quick Chart Generation Script

Run this script to generate all standard charts at once.
Usage: python quick_charts.py [--keyword KEYWORD] [--month MM] [--year YYYY]
"""

import argparse
import sys
from pathlib import Path
import pandas as pd
import plotly.graph_objects as go
import yaml

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from xminer.io.db import engine
from sqlalchemy import text


def load_config():
    """Load configuration from parameters.yml"""
    params_file = Path(__file__).parent.parent / 'src' / 'xminer' / 'config' / 'parameters.yml'

    with params_file.open('r', encoding='utf-8') as f:
        params = yaml.safe_load(f) or {}

    return params


def setup_graphics_dir(year, month):
    """Create and return graphics directory"""
    params = load_config()

    ym = f"{year:04d}{month:02d}"
    base_dir = Path(params.get('graphics_base_dir', '../outputs'))
    graphics_dir = base_dir / ym / 'graphics'
    graphics_dir.mkdir(parents=True, exist_ok=True)

    return graphics_dir


# Party colors (standardized)
PARTY_COLORS = {
    "CDU/CSU": "#000000",
    "SPD": "#E3000F",
    "GRÜNE": "#1AA64A",
    "BÜNDNIS 90/DIE GRÜNEN": "#1AA64A",
    "DIE LINKE.": "#BE3075",
    "FDP": "#FFED00",
    "AFD": "#009EE0",
    "BSW": "#009688",
}


def normalize_party(p: str) -> str:
    """Normalize party names"""
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
    """Get color for a party"""
    normalized = normalize_party(party)
    return PARTY_COLORS.get(normalized, "#888888")


def create_keyword_chart(keyword, year, month, output_dir):
    """
    Create bar chart for keyword analysis.

    Args:
        keyword: Keyword to search for
        year: Year to analyze
        month: Month to analyze
        output_dir: Directory to save output
    """
    query = f"""
    SELECT
        p.partei_kurz AS party,
        COUNT(*) AS tweet_count,
        SUM(t.impression_count) AS total_impressions
    FROM public.tweets t
    JOIN politicians_{month:02d}_{year} p ON t.username = p.username
    WHERE t.text ILIKE '%{keyword}%'
    GROUP BY p.partei_kurz
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        print(f"⚠️  No data found for keyword '{keyword}'")
        return None

    # Normalize party names
    df['party_norm'] = df['party'].apply(normalize_party)

    # Aggregate by normalized party
    df_agg = (
        df.groupby('party_norm')
        .agg({'tweet_count': 'sum', 'total_impressions': 'sum'})
        .reset_index()
        .sort_values('tweet_count', ascending=True)
    )

    # Create chart
    colors = [get_party_color(p) for p in df_agg['party_norm']]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=df_agg['party_norm'],
        x=df_agg['tweet_count'],
        orientation='h',
        marker_color=colors,
        text=[f"{v:,.0f}" for v in df_agg['tweet_count']],
        textposition='outside',
        textfont=dict(color='white', size=14)
    ))

    title = f"Tweets über '{keyword}' nach Partei<br><sub style='font-size:0.85em;'>Erhoben für {month:02d}/{year}</sub>"

    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor='center', font=dict(size=22)),
        xaxis_title="Anzahl Tweets",
        yaxis_title="",
        plot_bgcolor='#1a1a1a',
        paper_bgcolor='#1a1a1a',
        font=dict(color='white', size=14),
        margin=dict(l=120, r=100, t=120, b=60),
        height=max(400, 60 * len(df_agg)),
        xaxis=dict(gridcolor='#333333'),
        yaxis=dict(gridcolor='#333333', tickfont=dict(size=16))
    ))

    # Save
    output_file = output_dir / f"{keyword.lower()}_tweets_by_party.png"
    fig.write_image(output_file, width=1200, height=675, scale=2)

    print(f"✅ Created: {output_file.name}")
    print(f"   Total tweets: {df_agg['tweet_count'].sum():,}")
    print(f"   Parties: {', '.join(df_agg['party_norm'].tolist())}\n")

    return fig


def create_top_trends_chart(year, month, output_dir, top_n=15):
    """
    Create bar chart of top trends.

    Args:
        year: Year to analyze
        month: Month to analyze
        output_dir: Directory to save output
        top_n: Number of top trends to show
    """
    query = f"""
    SELECT
        trend_name,
        SUM(tweet_count) as total_tweets,
        COUNT(*) as appearances
    FROM public.x_trends
    WHERE
        EXTRACT(YEAR FROM retrieved_at) = {year}
        AND EXTRACT(MONTH FROM retrieved_at) = {month}
        AND place_name = 'Germany'
    GROUP BY trend_name
    ORDER BY total_tweets DESC
    LIMIT {top_n}
    """

    with engine.connect() as conn:
        df = pd.read_sql(text(query), conn)

    if df.empty:
        print(f"⚠️  No trends data found for {year}-{month:02d}")
        return None

    # Reverse for display
    df = df.iloc[::-1]

    # Create chart
    import plotly.express as px
    colors = px.colors.sequential.Viridis_r[:len(df)]

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=df['trend_name'],
        x=df['total_tweets'],
        orientation='h',
        marker_color=colors,
        text=[f"{v:,.0f}" for v in df['total_tweets']],
        textposition='outside',
        textfont=dict(color='white', size=12)
    ))

    title = f"Top {top_n} Trends nach Tweet-Volumen<br><sub style='font-size:0.85em;'>Erhoben für {month:02d}/{year}</sub>"

    fig.update_layout(
        title=dict(text=title, x=0.5, xanchor='center', font=dict(size=20)),
        xaxis_title="Anzahl Tweets",
        yaxis_title="",
        plot_bgcolor='#1a1a1a',
        paper_bgcolor='#1a1a1a',
        font=dict(color='white'),
        margin=dict(l=200, r=100, t=100, b=60),
        height=max(500, 40 * len(df)),
        xaxis=dict(gridcolor='#333333'),
        yaxis=dict(gridcolor='#333333')
    ))

    # Save
    output_file = output_dir / f"top_{top_n}_trends.png"
    fig.write_image(output_file, width=1200, height=675, scale=2)

    print(f"✅ Created: {output_file.name}")
    print(f"   Total trends: {len(df)}")
    print(f"   Top trend: {df.iloc[-1]['trend_name']} ({df.iloc[-1]['total_tweets']:,} tweets)\n")

    return fig


def main():
    """Main execution"""
    parser = argparse.ArgumentParser(description='Generate quick political charts')
    parser.add_argument('--keyword', type=str, help='Keyword to analyze (e.g., Venezuela)')
    parser.add_argument('--month', type=int, help='Month (1-12)')
    parser.add_argument('--year', type=int, help='Year (e.g., 2025)')
    parser.add_argument('--trends', action='store_true', help='Generate trends chart')
    parser.add_argument('--all', action='store_true', help='Generate all charts')

    args = parser.parse_args()

    # Load config for defaults
    params = load_config()
    year = args.year or params.get('year', 2025)
    month = args.month or params.get('month', 12)

    # Setup output directory
    output_dir = setup_graphics_dir(year, month)

    print(f"\n{'='*80}")
    print(f"Quick Charts Generator")
    print(f"{'='*80}\n")
    print(f"Period: {year}-{month:02d}")
    print(f"Output: {output_dir}\n")

    # Generate charts based on arguments
    if args.keyword or args.all:
        keyword = args.keyword or "Venezuela"
        print(f"Generating keyword chart for: {keyword}")
        create_keyword_chart(keyword, year, month, output_dir)

    if args.trends or args.all:
        print(f"Generating top trends chart")
        create_top_trends_chart(year, month, output_dir)

    if not (args.keyword or args.trends or args.all):
        print("No charts specified. Use --keyword, --trends, or --all")
        print("\nExamples:")
        print("  python quick_charts.py --keyword Venezuela")
        print("  python quick_charts.py --trends --month 11 --year 2025")
        print("  python quick_charts.py --all")
        parser.print_help()
        return

    print(f"\n{'='*80}")
    print(f"✅ All charts generated successfully!")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
