# How to Use the Visualization Notebooks

Quick guide for generating charts using Jupyter notebooks.

---

## 📊 Available Notebooks

### 1. **keyword_analysis_simple.ipynb** ⭐ RECOMMENDED
**Purpose:** Quick keyword analysis with bar and pie charts

**What it creates:**
- Bar chart showing tweet count by party
- Pie chart showing percentage distribution
- List of top 10 tweets by impressions

**How to use:**
1. Open the notebook in VS Code
2. Find this cell:
   ```python
   KEYWORD = "Venezuela"  # Change this to analyze different topics
   ```
3. Change `"Venezuela"` to your keyword (e.g., `"Ukraine"`, `"Klimawandel"`, `"Migration"`)
4. Click **"Run All"** (or press Shift+Enter on each cell)
5. Charts saved to: `outputs/202512/graphics/keywords/`

**Output files:**
- `{keyword}_tweets_by_party.png` (1200x675 - for X/Twitter)
- `{keyword}_distribution_pie.png` (1080x1080 - for Instagram)

---

### 2. **keyword_analysis.ipynb** (Advanced)
**Purpose:** Comprehensive keyword analysis with multiple visualizations

**What it creates:**
- Single keyword bar charts (tweets and impressions)
- Multi-keyword comparison charts
- Keyword heatmap by party
- Top tweets analysis

**How to use:**
1. Open the notebook
2. Find the configuration cells:
   ```python
   # For detailed analysis of one keyword
   SINGLE_KEYWORD = "Venezuela"

   # For comparing multiple keywords
   KEYWORDS = ["Venezuela", "Ukraine", "Klimawandel", "Migration"]
   ```
3. Modify the keywords as needed
4. Run all cells
5. Charts saved to: `outputs/202512/graphics/keywords/`

**Output files:**
- `{keyword}_tweets_by_party.png`
- `{keyword}_impressions_by_party.png`
- `keywords_comparison_by_party.png`
- `keywords_heatmap.png`

**Note:** This notebook can take longer to run if analyzing multiple keywords.

---

### 3. **trends_visualization.ipynb**
**Purpose:** Analyze trending topics from X (Twitter) trends data

**What it creates:**
- Word bubble chart (top trending words)
- Top trends bar chart
- Timeline showing trend evolution
- Category distribution pie chart

**How to use:**
1. Open the notebook
2. **No configuration needed** - automatically uses current month/year from config
3. Run all cells
4. Charts saved to: `outputs/202512/graphics/trends/`

**Output files:**
- `trending_words_x_twitter.png` (1200x675)
- `trending_words_instagram_square.png` (1080x1080)
- `top_trends_bar_x_twitter.png`
- `trends_timeline_x_twitter.png`
- `trends_categories_instagram_square.png`

**Data source:** `x_trends` table (must be populated by `fetch_x_trends` task first)

---

## 🚀 Quick Start Guide

### Step 1: Choose Your Notebook

**For keyword analysis (e.g., "Venezuela", "Ukraine"):**
→ Use `keyword_analysis_simple.ipynb` ✅

**For trends analysis (what's trending on X):**
→ Use `trends_visualization.ipynb` ✅

**For advanced multi-keyword comparison:**
→ Use `keyword_analysis.ipynb` ✅

### Step 2: Open in VS Code

1. In VS Code, navigate to `notebooks/`
2. Click on the notebook file
3. VS Code will open it with Jupyter interface

### Step 3: Configure (if needed)

**For keyword notebooks:** Change the `KEYWORD` variable
**For trends notebook:** No configuration needed

### Step 4: Run

Click **"Run All"** button at the top, or:
- Press `Shift + Enter` to run each cell one by one
- Click the play button (▶) next to each cell

### Step 5: Find Your Charts

Charts are automatically saved to:
- **Keyword charts:** `outputs/YYYYMM/graphics/keywords/`
- **Trends charts:** `outputs/YYYYMM/graphics/trends/`

The notebook will print the exact path when saving.

**Important:** Both the notebooks AND the quick_charts.py script save to the same `keywords/` subdirectory for consistency.

---

## 🎨 Chart Formats

### X/Twitter (Landscape)
- Size: 1200x675 pixels
- Ratio: 16:9
- Best for: Twitter posts

### Instagram (Square)
- Size: 1080x1080 pixels
- Ratio: 1:1
- Best for: Instagram feed posts

### Instagram (Portrait)
- Size: 1080x1350 pixels
- Ratio: 4:5
- Best for: Instagram stories

---

## ⚙️ Configuration

All notebooks read settings from: `src/xminer/config/parameters.yml`

**Key settings:**
```yaml
year: 2025
month: 12
graphics_base_dir: "../outputs"  # or absolute path
```

To change the period:
1. Edit `parameters.yml`
2. Change `year` and `month`
3. Re-run the notebook

---

## 🔧 Troubleshooting

### "No module named 'kaleido'"
**Solution:** Install kaleido
```bash
.venv/bin/python -m pip install kaleido
```
Then restart the kernel.

### "No data found for keyword"
**Reasons:**
1. No tweets mention that keyword
2. Wrong month/year selected
3. `politicians_MM_YYYY` table doesn't exist for that period

**Check data:**
```python
# In a notebook cell
query = "SELECT COUNT(*) FROM tweets WHERE text ILIKE '%YourKeyword%'"
with engine.connect() as conn:
    result = pd.read_sql(text(query), conn)
print(result)
```

### "Table politicians_12_2025 does not exist"
**Solution:** The politicians table for that month hasn't been created yet.
Check available tables:
```sql
SELECT table_name FROM information_schema.tables
WHERE table_name LIKE 'politicians_%'
ORDER BY table_name;
```

### "Permission denied"
**Solution:** Database user needs SELECT permission on `tweets` and `politicians_*` tables.

### "Kernel not found" or "Can't run cells"
**Solution:** Select the correct Python kernel
1. Click on kernel selector (top right in VS Code)
2. Choose: `xminer (Python 3.14.2)` or similar
3. If not available, select "Python Environments" and choose `.venv/bin/python`

### Charts not displaying in notebook
**Solution:**
1. Check that `kaleido` is installed
2. Restart kernel
3. Run all cells from the beginning
4. The last cell should display the saved images

### Can't find output files
**Check these locations:**
```bash
# From project root
ls outputs/202512/graphics/keywords/
ls outputs/202512/graphics/trends/
ls ../outputs/202512/graphics/  # If run from notebooks/ directory
```

The notebook prints the save path - look for lines like:
```
✅ Saved: outputs/202512/graphics/keywords/venezuela_tweets_by_party.png
```

---

## 💡 Tips

### Tip 1: Change Keywords Without Editing
Run this cell before running the analysis:
```python
KEYWORD = input("Enter keyword: ")
```

### Tip 2: Batch Process Multiple Keywords
Add this cell:
```python
keywords = ["Venezuela", "Ukraine", "Klimawandel", "Migration"]

for kw in keywords:
    KEYWORD = kw
    # Then run the analysis cells
```

### Tip 3: View Charts in Notebook
The last cell displays the saved charts:
```python
from IPython.display import Image, display
display(Image(filename='path/to/chart.png', width=800))
```

### Tip 4: Quick Testing
To test without saving files, comment out the `write_image` line:
```python
# fig.write_image(output_file, width=1200, height=675, scale=2)
fig.show()  # Just display, don't save
```

### Tip 5: Customize Colors
Party colors are defined at the top. To change:
```python
PARTY_COLORS = {
    "CDU/CSU": "#YOUR_COLOR_HERE",
    # ...
}
```

---

## 📝 Example Workflow

**Goal:** Analyze "Klimawandel" (climate change) by party

1. Open `keyword_analysis_simple.ipynb`
2. Change: `KEYWORD = "Klimawandel"`
3. Run all cells
4. Find charts in: `outputs/202512/graphics/keywords/`
5. Files created:
   - `klimawandel_tweets_by_party.png`
   - `klimawandel_distribution_pie.png`
6. Open files to view (or scroll to bottom of notebook)
7. Post on social media! 🎉

---

## 🚨 Common Mistakes

❌ **Forgetting to restart kernel after installing packages**
✅ Always restart kernel after `pip install`

❌ **Using wrong month/year in filename**
✅ Check `parameters.yml` for correct period

❌ **Not running cells in order**
✅ Always "Run All" or run cells from top to bottom

❌ **Misspelling keywords** (case matters for some searches)
✅ Use exact spelling, check with a test query first

❌ **Expecting instant results with complex queries**
✅ Multi-keyword analysis can take 30+ seconds

---

## 🎯 Next Steps

After generating your charts:

1. **Review the data** - Check the printed statistics
2. **Open the images** - Verify they look correct
3. **Edit if needed** - Adjust colors, titles, etc.
4. **Post on social media** - Charts are ready to share!

For more details, see: [README_VISUALIZATIONS.md](README_VISUALIZATIONS.md)

---

**Last Updated:** January 2026
**Questions?** Check the main documentation or review the notebook comments.
