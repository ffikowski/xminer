# Bundestag Votes Implementation Summary

## Overview

Successfully implemented an automated script to fetch Bundestag member votes from Excel files and enrich them with law metadata scraped from the Bundestag website.

## Key Feature: Automatic Law Name Extraction

The script now **automatically fetches law names and metadata** from the Bundestag website listing page, so you don't need to manually track which Excel file corresponds to which law.

## How It Works

1. **Scrapes Bundestag Website**: Fetches the official listing page at `https://www.bundestag.de/parlament/plenum/abstimmung/liste`
2. **Extracts Metadata**: Parses the AJAX-loaded data to extract:
   - Law/topic title (e.g., "Haushaltsgesetz 2026")
   - Vote date (e.g., "2025-11-28")
   - Source URL for the Excel file
3. **Matches Files**: Maps each downloaded Excel file to its metadata by filename
4. **Enriches Database**: Stores the law name, date, and URL alongside each vote record

## Implementation Details

### Files Modified/Created

1. **src/xminer/tasks/fetch_bundestag_votes.py** - Enhanced with:
   - `fetch_vote_metadata_from_bundestag()` - Scrapes Bundestag AJAX endpoint
   - Updated `transform_to_db_format()` - Adds metadata to records
   - Updated `main()` - Integrates metadata fetching into workflow

2. **data/bundestag_votes_schema.sql** - Added columns:
   - `vote_title TEXT` - Law/topic name
   - `vote_date DATE` - Date of vote
   - `vote_source_url TEXT` - URL to Excel file
   - Added indexes for efficient queries

3. **pyproject.toml** - Added dependencies:
   - `requests` - HTTP requests
   - `beautifulsoup4` - HTML parsing (available if needed)
   - `openpyxl` - Excel file reading

4. **BUNDESTAG_VOTES_SETUP.md** - Updated documentation

### Database Schema

```sql
CREATE TABLE bundestag_votes (
    -- Vote identifiers
    wahlperiode INTEGER NOT NULL,
    sitzungnr INTEGER NOT NULL,
    abstimmnr INTEGER NOT NULL,

    -- Member information
    fraktion_gruppe VARCHAR(50),
    name VARCHAR(100) NOT NULL,
    vorname VARCHAR(100),
    titel VARCHAR(50),
    bezeichnung TEXT,

    -- Vote result (binary flags)
    ja INTEGER NOT NULL DEFAULT 0,
    nein INTEGER NOT NULL DEFAULT 0,
    enthaltung INTEGER NOT NULL DEFAULT 0,
    ungueltig INTEGER NOT NULL DEFAULT 0,
    nichtabgegeben INTEGER NOT NULL DEFAULT 0,

    -- Metadata
    bemerkung TEXT,

    -- **NEW: Law metadata from website**
    vote_title TEXT,
    vote_date DATE,
    vote_source_url TEXT,

    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Metadata Scraping Logic

The script uses regex to parse the Bundestag AJAX endpoint:

```python
# AJAX URL with pagination
ajax_url = "https://www.bundestag.de/ajax/filterlist/de/parlament/plenum/abstimmung/liste/462112-462112"

# Regex pattern to extract vote entries from HTML
pattern = r'<p><strong>\s*(\d{2}\.\d{2}\.\d{4}):\s*<span>([^<]+)</span>\s*</strong></p>.*?href="([^"]+\.xlsx)"'

# Returns dict mapping filenames to metadata
{
    "20251219_2_xls.xlsx": {
        "title": "Gesetzentwurf zum Verbrauchervertrags- und Versicherungsvertragsrecht",
        "date": "2025-12-19",
        "url": "https://www.bundestag.de/resource/blob/1134414/20251219_2_xls.xlsx"
    },
    ...
}
```

## Usage

### Prerequisites

1. **Install dependencies**:
   ```bash
   pip install openpyxl requests beautifulsoup4
   ```

2. **Set up .env file** with database credentials:
   ```env
   DATABASE_URL=postgresql+psycopg2://user:password@host:port/database
   X_BEARER_TOKEN=your_token
   ```

3. **Place Excel files** in `data/bundestag_votes/`

### Run the Script

```bash
# Option 1: Run directly
python -m xminer.tasks.fetch_bundestag_votes

# Option 2: Run via pipeline
python -m xminer.pipelines.cli run fetch
```

### What Happens

```
================================================================================
Starting Bundestag votes fetch
================================================================================
Configuration:
  Excel directory: data/bundestag_votes
  Load to database: True
  Store CSV: False
Ensuring bundestag_votes table exists...
Table ready.
Fetching vote metadata from Bundestag website...
Fetching metadata page (offset=0)...
Extracted 23 vote entries from this page
Fetching metadata page (offset=30)...
Extracted 23 vote entries from this page
...
Fetched metadata for 986 votes from Bundestag website
Found 1 Excel files in data/bundestag_votes
Parsing Excel file: 20251219_2_xls.xlsx
Loaded 630 vote records from 20251219_2_xls.xlsx
Found metadata: Gesetzentwurf zum Verbrauchervertrags- und Versicherungsvertragsrecht
Transforming 630 records to database format...
Transformed 630 records
Upserting 630 vote records to database...
Successfully upserted 630 records
================================================================================
Bundestag votes fetch complete!
  Files processed: 1
  Total records: 630
================================================================================
```

## Example Queries

### Get all votes for a specific law

```sql
SELECT
    name, vorname, fraktion_gruppe,
    CASE
        WHEN ja = 1 THEN 'Ja'
        WHEN nein = 1 THEN 'Nein'
        WHEN enthaltung = 1 THEN 'Enthaltung'
        WHEN nichtabgegeben = 1 THEN 'Nicht abgegeben'
        ELSE 'Ungültig'
    END as vote
FROM bundestag_votes
WHERE vote_title LIKE '%Haushaltsgesetz%'
ORDER BY fraktion_gruppe, name;
```

### Count votes by party for a specific law

```sql
SELECT
    fraktion_gruppe,
    SUM(ja) as yes_votes,
    SUM(nein) as no_votes,
    SUM(enthaltung) as abstentions,
    COUNT(*) as total_members
FROM bundestag_votes
WHERE vote_title = 'Haushaltsgesetz 2026'
GROUP BY fraktion_gruppe;
```

### Find all votes on a specific date

```sql
SELECT DISTINCT
    vote_title,
    wahlperiode,
    sitzungnr,
    abstimmnr,
    COUNT(*) as member_count
FROM bundestag_votes
WHERE vote_date = '2025-12-19'
GROUP BY vote_title, wahlperiode, sitzungnr, abstimmnr;
```

### Search for laws by keyword

```sql
SELECT DISTINCT
    vote_date,
    vote_title,
    COUNT(*) as votes_cast
FROM bundestag_votes
WHERE vote_title ILIKE '%klimapolitik%'
GROUP BY vote_date, vote_title
ORDER BY vote_date DESC;
```

## Benefits

1. **Fully Automated**: No manual metadata entry required
2. **Scalable**: Can process hundreds of votes automatically
3. **Searchable**: Query votes by law name, date, or keywords
4. **Traceable**: Stores source URL for each vote
5. **Up-to-date**: Fetches latest metadata from official Bundestag website

## Next Steps

1. Create `.env` file with your database credentials
2. Download Excel files from https://www.bundestag.de/parlament/plenum/abstimmung/liste
3. Place them in `data/bundestag_votes/`
4. Run the script
5. Query the database to analyze voting patterns!

## Technical Notes

- **Pagination**: Fetches up to 1000 votes by default (configurable via `limit` parameter)
- **Error Handling**: Continues processing even if metadata fetch fails
- **Upsert Logic**: Updates existing records if vote already in database
- **Performance**: Fetches metadata once at start, then processes all Excel files

For full documentation, see [BUNDESTAG_VOTES_SETUP.md](BUNDESTAG_VOTES_SETUP.md).
