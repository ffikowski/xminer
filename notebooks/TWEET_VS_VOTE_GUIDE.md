# Tweet vs Vote Analysis Guide

## Overview

This notebook compares what politicians **tweet about** versus how they **actually vote** on important topics in the Bundestag. It creates side-by-side visualizations showing:

- **Left chart**: How many tweets each party posted about a topic
- **Right chart**: How each party voted on related legislation (Ja vs Nein)

## Quick Start

1. **Open the notebook**: `tweet_vs_vote_analysis.ipynb`

2. **Set your topic** in the configuration cell:
   ```python
   TOPIC_KEYWORD = "Ukraine"   # For searching tweets
   VOTE_KEYWORD = "Bundeswehr" # For searching vote titles
   ```

3. **Run all cells** (Shift + Enter or "Run All")

4. **Find your charts** in: `outputs/202512/graphics/tweet_vs_vote/`

## Good Topics to Analyze

Based on available Bundestag votes, here are interesting topics:

### Military & Foreign Policy
- **Bundeswehr** - Military deployments
- **Ukraine** / **Russland** - Ukraine/Russia policy
- **Afghanistan** - Afghan refugee policy

### Domestic Policy
- **Klima** - Climate policy
- **Mieten** - Rent control
- **Erbschaft** - Inheritance tax
- **Diesel** - Agricultural diesel subsidies

### Energy & Environment
- **AKW** / **Atomkraft** - Nuclear power
- **Verbrenner** - Combustion engine ban
- **Klimaschutz** - Climate protection

## Example Usage

### Example 1: Military Deployments
```python
TOPIC_KEYWORD = "Bundeswehr"
VOTE_KEYWORD = "Bundeswehr"
```

**Shows**: Do parties that tweet about military matters vote for deployments?

### Example 2: Ukraine Support
```python
TOPIC_KEYWORD = "Ukraine"
VOTE_KEYWORD = "russisch"  # Russian-related votes
```

**Shows**: Correlation between Ukraine tweets and votes on Russian assets/sanctions

### Example 3: Climate Policy
```python
TOPIC_KEYWORD = "Klima"
VOTE_KEYWORD = "Klima"
```

**Shows**: Do parties tweeting about climate actually vote for climate measures?

## Understanding the Output

### Left Chart: Tweet Activity
- Shows total number of tweets mentioning the topic
- Colored by party (using standard party colors)
- Higher bars = more social media attention to the topic

### Right Chart: Voting Behavior
- **Green bars**: Ja (Yes) votes
- **Red bars**: Nein (No) votes
- Stacked to show split within each party
- Shows actual legislative action on the topic

### Key Insights
Look for:
- **High tweets, low votes**: Lots of talk, little action
- **Mismatched positions**: Party tweets supporting something but votes against it
- **Consistent parties**: Tweet activity aligns with voting behavior
- **Silent actors**: Low tweets but active in voting

## Output Files

Charts are saved as high-resolution PNGs optimized for social media:
- **Format**: 1600x900px (16:9 landscape)
- **Location**: `outputs/202512/graphics/tweet_vs_vote/`
- **Filename**: `{topic}_tweet_vs_vote.png`

## Customization

### Change Date Range
Modify in the configuration cell:
```python
START_DATE = "2025-12-01"
END_DATE = "2026-01-31"
```

### Change Chart Colors
Vote colors are hardcoded:
- Ja (Yes): `#00AA00` (green)
- Nein (No): `#CC0000` (red)

Party colors use standard German party colors (defined in the party colors cell).

## Troubleshooting

### "No votes found"
The notebook will:
1. Show a warning if no votes match your `VOTE_KEYWORD`
2. Display a list of available vote titles to help you find the right keyword

Try:
- Using shorter/broader keywords
- Searching for German terms
- Checking the available votes list

### "No tweets found"
- Check your `TOPIC_KEYWORD` spelling
- Try broader terms
- Verify the date range includes tweet activity

### Missing politicians
- Only politicians in `politicians_12_2025` table are included
- 66% of votes have usernames linked
- Unmatched votes are excluded from the analysis

## Advanced: Finding Related Votes

To see all available vote titles:
```sql
SELECT DISTINCT vote_title
FROM bundestag_votes
WHERE vote_title IS NOT NULL
ORDER BY vote_title
```

Or search for specific topics:
```sql
SELECT DISTINCT vote_title
FROM bundestag_votes
WHERE vote_title ILIKE '%klima%'
ORDER BY vote_title
```

## Tips for Social Media

1. **Choose controversial topics** for maximum engagement
2. **Look for contradictions** between tweets and votes
3. **Highlight specific parties** that talk much but vote differently
4. **Use during election campaigns** to fact-check party positions
5. **Post during relevant news cycles** for maximum impact

## Technical Details

### Data Sources
- **Tweets**: `tweets` table (Dec 2025 - Jan 2026)
- **Votes**: `bundestag_votes` table (full year 2025)
- **Politicians**: `politicians_12_2025` table

### Matching Logic
- Tweets matched by `text ILIKE '%keyword%'`
- Votes matched by `vote_title ILIKE '%keyword%'`
- Parties normalized (CDU/CSU merged, Grüne variants unified, etc.)
- Username linkage provides connection between tweets and votes

### Performance
- Fast queries (under 5 seconds)
- Chart generation: ~2-3 seconds
- Total notebook runtime: ~10-15 seconds
