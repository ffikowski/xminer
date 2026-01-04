# Visualization Notebooks Guide

This directory contains Jupyter notebooks for creating standardized visualizations for social media posting (X/Twitter and Instagram).

## Available Notebooks

### 1. `trends_visualization.ipynb` - Trends Analysis
**Purpose:** Visualize trending topics from X (Twitter) trends data.

**Charts Created:**
- **Word Bubble Chart** - Most mentioned words in trends (alternative to word cloud)
- **Top Trends Bar Chart** - Horizontal bar chart of top trends by tweet volume
- **Timeline Chart** - Evolution of trends over time (line chart)
- **Category Pie Chart** - Distribution of trending topics by category

**Output Formats:**
- X/Twitter: 1200x675px (16:9 landscape)
- Instagram Square: 1080x1080px (1:1)
- Instagram Portrait: 1080x1350px (4:5)

**How to Use:**
```python
# 1. Open the notebook
# 2. Run all cells to load data from database
# 3. Charts are automatically saved to: outputs/YYYYMM/graphics/trends/
# 4. Files are named with format suffix (e.g., trending_words_x_twitter.png)
```

**Data Source:** `x_trends` table (populated by `fetch_x_trends.py`)

---

### 2. `keyword_analysis.ipynb` - Keyword-Based Analysis
**Purpose:** Analyze how different political parties discuss specific topics/keywords.

**Charts Created:**
- **Single Keyword Bar Chart** - Tweets by party for a specific keyword
- **Impressions Bar Chart** - Reach of keyword tweets by party
- **Multi-Keyword Comparison** - Grouped bar chart comparing multiple keywords
- **Keyword Heatmap** - Intensity heatmap of keyword usage by party

**Example Use Cases:**
- Analyze "Venezuela" mentions by party
- Compare "Ukraine", "Klimawandel", "Migration" discussions
- Track which parties focus on which topics

**How to Use:**
```python
# 1. Configure keywords to analyze:
KEYWORDS = ["Venezuela", "Ukraine", "Klimawandel", "Migration"]
SINGLE_KEYWORD = "Venezuela"  # For detailed analysis

# 2. Run all cells
# 3. Charts saved to: outputs/YYYYMM/graphics/keywords/
```

**Data Source:** `tweets` table joined with `politicians_MM_YYYY` table

**Example Query:**
```sql
SELECT
    p.partei_kurz AS party,
    COUNT(*) AS venezuela_tweets
FROM public.tweets t
JOIN politicians_12_2025 p ON t.username = p.username
WHERE t.text ILIKE '%Venezuela%'
GROUP BY p.partei_kurz
ORDER BY venezuela_tweets DESC;
```

---

### 3. `posts_aggregated.ipynb` - Tweet Metrics by Party
**Purpose:** Aggregate tweet metrics and create party-wise visualizations.

**Charts Created:**
- **Pie Charts** - Party distribution by followers, impressions, etc.
- **Stacked Bar Charts** - Tweets vs. Impressions comparison
- **Horizontal Bar Charts** - Top politicians by metric

**Functions Available:**
- `plot_party_pie_pct()` - Pie chart with percentages
- `plot_party_stack_tweets_engagement()` - Stacked bars
- `plot_party_hbar()` - Top N profiles ranked

---

## Configuration

All notebooks load settings from: `src/xminer/config/parameters.yml`

**Key Parameters:**
```yaml
year: 2025
month: 12
graphics_base_dir: "/path/to/outputs"
```

**Graphics Output Structure:**
```
outputs/
└── YYYYMM/
    └── graphics/
        ├── trends/           # From trends_visualization.ipynb
        │   ├── trending_words_x_twitter.png
        │   ├── top_trends_bar_x_twitter.png
        │   ├── trends_timeline_x_twitter.png
        │   └── trends_categories_instagram_square.png
        ├── keywords/         # From keyword_analysis.ipynb
        │   ├── venezuela_tweets_by_party.png
        │   ├── venezuela_impressions_by_party.png
        │   ├── keywords_comparison_by_party.png
        │   └── keywords_heatmap.png
        └── ...               # Other charts
```

---

## Party Colors (Standardized)

All visualizations use consistent party colors:

| Party | Color | Hex Code |
|-------|-------|----------|
| CDU/CSU | Black | #000000 |
| SPD | Red | #E3000F |
| GRÜNE | Green | #1AA64A |
| DIE LINKE. | Purple | #BE3075 |
| FDP | Yellow | #FFED00 |
| AFD | Blue | #009EE0 |
| BSW | Teal | #009688 |

---

## Social Media Image Sizes

### X (Twitter)
- **Landscape:** 1200x675px (16:9) - Best for single images
- **Square:** 1080x1080px (1:1) - Also supported

### Instagram
- **Square:** 1080x1080px (1:1) - Feed posts
- **Portrait:** 1080x1350px (4:5) - Best for Stories/Reels
- **Landscape:** 1080x566px (1.91:1) - Supported but less common

### Recommendation
- Use **X/Twitter landscape (1200x675)** for Twitter posts
- Use **Instagram square (1080x1080)** for Instagram feed
- All exports are 2x scale (high resolution) for crisp display

---

## Dependencies

Required packages (already in `pyproject.toml`):
- `pandas` - Data manipulation
- `plotly` - Interactive visualizations
- `kaleido` - PNG export for Plotly
- `sqlalchemy` - Database connection
- `psycopg2` - PostgreSQL driver
- `pyyaml` - Config file parsing

Install missing packages:
```bash
pip install plotly kaleido
```

---

## Quick Start

1. **Set up database connection:**
   ```bash
   # Ensure .env file has DATABASE_URL
   DATABASE_URL=postgresql+psycopg2://user:password@host/db
   ```

2. **Configure period in parameters.yml:**
   ```yaml
   year: 2025
   month: 12
   ```

3. **Run notebook:**
   ```bash
   # Open Jupyter
   jupyter notebook

   # Or use VS Code with Jupyter extension
   # Open .ipynb file and run all cells
   ```

4. **Find your charts:**
   ```bash
   ls outputs/202512/graphics/trends/
   ls outputs/202512/graphics/keywords/
   ```

---

## Tips for Social Media Posting

### X/Twitter
- Use landscape format (1200x675)
- Add text overlay with title/key insight
- Include source: "Daten: PoliMetrics"
- Use relevant hashtags: #Bundestag #Politik #DataViz

### Instagram
- Use square format (1080x1080) for feed
- Can post multiple charts as carousel
- Add caption explaining the visualization
- Use story mode for portrait charts
- Tag relevant accounts

### Best Practices
1. **Keep it simple** - One clear message per chart
2. **Use high contrast** - Dark backgrounds work well
3. **Label clearly** - All axes and data points labeled
4. **Add context** - Include date range and source
5. **Test mobile** - Ensure text is readable on small screens

---

## Troubleshooting

### "No data found"
- Check that `x_trends` or `tweets` table has data for the specified month
- Run fetch tasks first: `python -m xminer.tasks.fetch_x_trends`

### "Table not found"
- Ensure `politicians_MM_YYYY` table exists for your month
- Check table name format: `politicians_12_2025` (not `politicians_12_2024`)

### "Permission denied"
- Check database user has SELECT permission on tables
- See `setup_bundestag_votes_db.sql` for permission setup example

### PNG export fails
- Install kaleido: `pip install kaleido`
- On Mac: May need to install system dependencies

---

## Contributing

To add new visualizations:

1. Create functions in existing notebooks or new notebook
2. Follow naming convention: `<topic>_visualization.ipynb`
3. Use standardized party colors and export sizes
4. Save to appropriate subdirectory in `graphics/`
5. Update this README with new chart descriptions

---

## Examples

### Venezuela Analysis
```python
# keyword_analysis.ipynb
SINGLE_KEYWORD = "Venezuela"
# Run notebook → Creates:
# - venezuela_tweets_by_party.png
# - venezuela_impressions_by_party.png
```

### Trending Words
```python
# trends_visualization.ipynb
# Run notebook → Creates:
# - trending_words_x_twitter.png (1200x675)
# - trending_words_instagram_square.png (1080x1080)
```

### Top Politicians
```python
# posts_aggregated.ipynb
fig = plot_party_hbar(
    df_profiles,
    y_col="username",
    x_col="followers_count",
    top_n=10,
    title="Top 10 Politiker nach Followern",
    save_name="top_politicians_followers"
)
```

---

## Support

For questions or issues:
- Check logs: `logs/fetch_x_profiles.log`
- Review database schema: `data/*.sql`
- See implementation docs: `BUNDESTAG_VOTES_IMPLEMENTATION_SUMMARY.md`

---

**Last Updated:** January 2026
**Author:** PoliMetrics Team
