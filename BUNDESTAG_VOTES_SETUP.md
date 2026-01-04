# Bundestag Votes Import Setup

This document explains how to use the Bundestag votes import script to fetch vote data from Excel files and import them into your database.

## Overview

The script processes Excel files containing Bundestag member voting records from plenary sessions. Each Excel file contains individual member votes for a specific voting session.

## Files Created

1. **Script**: [src/xminer/tasks/fetch_bundestag_votes.py](src/xminer/tasks/fetch_bundestag_votes.py)
2. **Database Schema**: [data/bundestag_votes_schema.sql](data/bundestag_votes_schema.sql)
3. **Configuration**: Updated [src/xminer/config/parameters.yml](src/xminer/config/parameters.yml)

## Database Schema

The `bundestag_votes` table has the following structure:

| Column | Type | Description |
|--------|------|-------------|
| `vote_id` | BIGSERIAL | Primary key |
| `wahlperiode` | INTEGER | Electoral period (e.g., 21) |
| `sitzungnr` | INTEGER | Session number (e.g., 51) |
| `abstimmnr` | INTEGER | Vote number within session (e.g., 2) |
| `fraktion_gruppe` | VARCHAR(50) | Party/faction (e.g., 'CDU/CSU', 'SPD') |
| `name` | VARCHAR(100) | Member's last name |
| `vorname` | VARCHAR(100) | Member's first name |
| `titel` | VARCHAR(50) | Title (e.g., 'Dr.', 'von') |
| `bezeichnung` | TEXT | Full display name |
| `ja` | INTEGER | 1 if voted yes, 0 otherwise |
| `nein` | INTEGER | 1 if voted no, 0 otherwise |
| `enthaltung` | INTEGER | 1 if abstained, 0 otherwise |
| `ungueltig` | INTEGER | 1 if invalid vote, 0 otherwise |
| `nichtabgegeben` | INTEGER | 1 if did not vote, 0 otherwise |
| `bemerkung` | TEXT | Remarks (e.g., 'gesetzlicher Mutterschutz') |
| `vote_title` | TEXT | **Law/topic title** (automatically fetched from website) |
| `vote_date` | DATE | **Date of the vote** (automatically fetched from website) |
| `vote_source_url` | TEXT | **URL to Excel file** (automatically fetched from website) |
| `retrieved_at` | TIMESTAMPTZ | When the record was imported |

**Constraints**:
- Unique constraint on `(wahlperiode, sitzungnr, abstimmnr, name, vorname)`
- Check constraint ensures exactly one vote type is selected

## Setup Instructions

### 1. Install Dependencies

The following libraries have been added to `pyproject.toml`:
- `openpyxl` - For reading Excel files
- `requests` - For HTTP requests to Bundestag website
- `beautifulsoup4` - For parsing HTML (if needed)

Install them:

```bash
# If using pip
pip install openpyxl requests beautifulsoup4

# Or reinstall all dependencies
pip install -e .
```

### 2. Configure Environment Variables

Create a `.env` file in the project root (if it doesn't exist):

```bash
cp .env.example .env
```

Edit `.env` and add your database credentials:

```env
DATABASE_URL=postgresql+psycopg2://user:password@host:port/database
X_BEARER_TOKEN=your_token  # Required by config, but not used for bundestag votes
ENV=dev
```

### 3. Download Excel Files

Download Bundestag vote Excel files from:
https://www.bundestag.de/parlament/plenum/abstimmung/liste

Place all Excel files (.xlsx or .xls) in the directory:
```
data/bundestag_votes/
```

**Example file naming**: `20251219_2_xls.xlsx`

### 4. Configure Parameters (Optional)

Edit [src/xminer/config/parameters.yml](src/xminer/config/parameters.yml) if needed:

```yaml
fetch_bundestag_votes:
  excel_dir: "data/bundestag_votes"  # Directory with Excel files
  load_to_db: true                   # Import to database
  store_csv: false                   # Optionally save combined CSV
```

## Running the Script

### Option 1: Run Directly

```bash
python -m xminer.tasks.fetch_bundestag_votes
```

### Option 2: Run via Pipeline

The script has been integrated into the fetch pipeline:

```bash
# Run all fetch tasks (including bundestag votes)
python -m xminer.pipelines.cli run fetch

# Or run the entire pipeline
python -m xminer.pipelines.cli run all
```

### Option 3: Run Individual Task

```bash
cd src
python -m xminer.tasks.fetch_bundestag_votes
```

## What the Script Does

1. **Creates the table**: Creates `bundestag_votes` table if it doesn't exist
2. **Fetches metadata**: Scrapes the Bundestag website to get law names, dates, and URLs for all votes
3. **Finds Excel files**: Scans `data/bundestag_votes/` for .xlsx/.xls files
4. **Matches metadata**: Links each Excel file to its corresponding law name/date from the website
5. **Parses each file**: Reads vote data from each Excel file
6. **Enriches data**: Adds law name, date, and source URL to each vote record
7. **Upserts to database**: Inserts new records or updates existing ones
8. **Logs progress**: Writes detailed logs to `logs/fetch_x_profiles.log`

## Excel File Structure Expected

The script expects Excel files with these columns:

- `Wahlperiode` - Electoral period number
- `Sitzungnr` - Session number
- `Abstimmnr` - Vote number
- `Fraktion/Gruppe` - Party/faction
- `Name` - Last name
- `Vorname` - First name
- `Titel` - Title
- `ja` - Yes vote (1/0)
- `nein` - No vote (1/0)
- `Enthaltung` - Abstention (1/0)
- `ungültig` - Invalid vote (1/0)
- `nichtabgegeben` - Did not vote (1/0)
- `Bezeichnung` - Display name
- `Bemerkung` - Remarks

## Example Queries

### Get all votes for a specific session

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
WHERE wahlperiode = 21
  AND sitzungnr = 51
  AND abstimmnr = 2
ORDER BY fraktion_gruppe, name;
```

### Count votes by party for a session

```sql
SELECT
    fraktion_gruppe,
    SUM(ja) as yes_votes,
    SUM(nein) as no_votes,
    SUM(enthaltung) as abstentions,
    SUM(nichtabgegeben) as did_not_vote,
    COUNT(*) as total_members
FROM bundestag_votes
WHERE wahlperiode = 21
  AND sitzungnr = 51
  AND abstimmnr = 2
GROUP BY fraktion_gruppe
ORDER BY fraktion_gruppe;
```

### Get voting history for a specific member

```sql
SELECT
    wahlperiode, sitzungnr, abstimmnr,
    fraktion_gruppe,
    CASE
        WHEN ja = 1 THEN 'Ja'
        WHEN nein = 1 THEN 'Nein'
        WHEN enthaltung = 1 THEN 'Enthaltung'
        WHEN nichtabgegeben = 1 THEN 'Nicht abgegeben'
        ELSE 'Ungültig'
    END as vote,
    retrieved_at
FROM bundestag_votes
WHERE name = 'Scholz'
  AND vorname = 'Olaf'
ORDER BY wahlperiode DESC, sitzungnr DESC, abstimmnr DESC;
```

## Troubleshooting

### ModuleNotFoundError: No module named 'openpyxl'

Install the dependency:
```bash
pip install openpyxl
```

### RuntimeError: Missing required env vars

Create a `.env` file with your database credentials (see Setup step 2).

### No Excel files found

Ensure you've placed Excel files in `data/bundestag_votes/` directory.

### Database connection error

Check your `DATABASE_URL` in `.env` file is correct.

## Output

The script logs to:
- **Console**: Real-time progress
- **File**: `logs/fetch_x_profiles.log`

Example output:
```
2026-01-04 23:45:12 [INFO] ================================================================================
2026-01-04 23:45:12 [INFO] Starting Bundestag votes fetch
2026-01-04 23:45:12 [INFO] ================================================================================
2026-01-04 23:45:12 [INFO] Configuration:
2026-01-04 23:45:12 [INFO]   Excel directory: data/bundestag_votes
2026-01-04 23:45:12 [INFO]   Load to database: True
2026-01-04 23:45:12 [INFO]   Store CSV: False
2026-01-04 23:45:12 [INFO] Ensuring bundestag_votes table exists...
2026-01-04 23:45:12 [INFO] Table ready.
2026-01-04 23:45:12 [INFO] Found 1 Excel files in data/bundestag_votes
2026-01-04 23:45:12 [INFO] Parsing Excel file: 20251219_2_xls.xlsx
2026-01-04 23:45:12 [INFO] Loaded 630 vote records from 20251219_2_xls.xlsx
2026-01-04 23:45:12 [INFO] Transforming 630 records to database format...
2026-01-04 23:45:12 [INFO] Transformed 630 records
2026-01-04 23:45:12 [INFO] Upserting 630 vote records to database...
2026-01-04 23:45:13 [INFO] Successfully upserted 630 records
2026-01-04 23:45:13 [INFO] ================================================================================
2026-01-04 23:45:13 [INFO] Bundestag votes fetch complete!
2026-01-04 23:45:13 [INFO]   Files processed: 1
2026-01-04 23:45:13 [INFO]   Total records: 630
2026-01-04 23:45:13 [INFO] ================================================================================
```

## Next Steps

1. Download all historical Excel files from the Bundestag website
2. Place them in `data/bundestag_votes/`
3. Run the script to import all votes
4. Set up a periodic job to download and import new votes as they become available

## Support

For issues or questions, check the logs at `logs/fetch_x_profiles.log` or review the source code at [src/xminer/tasks/fetch_bundestag_votes.py](src/xminer/tasks/fetch_bundestag_votes.py).
