# Bundestag 2025 Election Results

This document describes how to fetch and use the official Bundestag 2025 election results data.

## Data Source

Official data from the Federal Returning Officer (Bundeswahlleiterin):
- **Website**: https://www.bundeswahlleiterin.de/bundestagswahlen/2025/ergebnisse/opendata.html
- **CSV Data**: https://www.bundeswahlleiterin.de/bundestagswahlen/2025/ergebnisse/opendata/btw25/csv/

## Available Data Files

### kerg.csv
- **Description**: Overall results from all areas (federal territory, federal states, and electoral districts)
- **Format**: Tabular form with absolute values only
- **Size**: ~160KB
- **Use Case**: Basic election results, vote counts per party/candidate

### kerg2.csv
- **Description**: Overall results in flat form
- **Format**: Includes absolute values, relative values (percentages), and differences to previous election
- **Size**: ~1.7MB
- **Use Case**: Detailed analysis including vote share changes, comparative analysis

## Database Schema

The data is stored in PostgreSQL tables using JSONB format for flexibility:

### bundestag_2025_kerg
```sql
CREATE TABLE bundestag_2025_kerg (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### bundestag_2025_kerg2
```sql
CREATE TABLE bundestag_2025_kerg2 (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    retrieved_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## Setup

### 1. Create Database Tables

Run the SQL setup script as a database superuser:

```bash
psql -U postgres -d your_database -f setup_bundestag_2025_db.sql
```

### 2. Configure Data Directory

Edit `src/xminer/config/parameters.yml`:

```yaml
bundestag_2025_results:
  data_dir: "data/bundestag_2025"  # Directory for CSV files
```

## Usage

### Fetch and Load Data

Run the fetch script:

```bash
# From project root
python -m xminer.tasks.fetch_bundestag_2025_results
```

This will:
1. Download `kerg.csv` and `kerg2.csv` from bundeswahlleiterin.de
2. Save CSV files to the configured data directory
3. Load data into PostgreSQL tables as JSONB
4. Create indexes for efficient querying

### Query Examples

#### Get all data from kerg.csv

```sql
SELECT * FROM bundestag_2025_kerg LIMIT 10;
```

#### Query specific fields from JSONB data

```sql
-- Example: Get party results (adjust field names based on actual CSV structure)
SELECT
    data->>'Gebiet' as area,
    data->>'Partei' as party,
    data->>'Stimmen' as votes
FROM bundestag_2025_kerg
WHERE data->>'Gebiet' IS NOT NULL;
```

#### Search within JSONB data

```sql
-- Example: Find all records for a specific party
SELECT * FROM bundestag_2025_kerg2
WHERE data @> '{"Partei": "SPD"}';
```

#### Get latest data

```sql
SELECT * FROM bundestag_2025_kerg2
ORDER BY retrieved_at DESC
LIMIT 100;
```

## Data Updates

The election results are updated progressively as votes are counted. To fetch the latest data:

```bash
# Re-run the fetch script
python -m xminer.tasks.fetch_bundestag_2025_results
```

The script will:
- Download the latest versions of the CSV files
- Insert new data (duplicates are automatically skipped)
- Preserve historical data with timestamps

## Integration with Politicians Data

To link election results with politician profiles:

```sql
-- Example: Join with politicians table
SELECT
    p.vorname,
    p.nachname,
    p.partei_kurz,
    k.data->>'Stimmen' as votes
FROM politicians_12_2025 p
LEFT JOIN bundestag_2025_kerg k
    ON k.data->>'Kandidat' = CONCAT(p.vorname, ' ', p.nachname)
WHERE p.partei_kurz IS NOT NULL;
```

## CSV File Structure

The CSV files use semicolon (`;`) as delimiter and may use ISO-8859-1 or UTF-8 encoding.

The script automatically handles:
- Encoding detection (tries UTF-8, falls back to ISO-8859-1)
- Delimiter parsing
- NULL value handling
- JSONB conversion

## Troubleshooting

### Download Fails

```
❌ Failed to download: Connection timeout
```

**Solution**: Check internet connection and firewall settings. The bundeswahlleiterin.de site may experience high traffic during election periods.

### Encoding Issues

```
UnicodeDecodeError: 'utf-8' codec can't decode byte
```

**Solution**: The script automatically tries ISO-8859-1 encoding. If issues persist, manually inspect the CSV file encoding.

### Duplicate Key Errors

```
CONSTRAINT unique_kerg_row violated
```

**Solution**: This is expected behavior. The script uses `ON CONFLICT DO NOTHING` to skip duplicates automatically.

## Advanced Usage

### Export Data to CSV

```sql
-- Export results to CSV file
COPY (
    SELECT
        data->>'Gebiet' as area,
        data->>'Partei' as party,
        data->>'Stimmen' as votes
    FROM bundestag_2025_kerg
) TO '/path/to/output.csv' WITH CSV HEADER;
```

### Create Materialized View

For faster queries, create a materialized view:

```sql
CREATE MATERIALIZED VIEW bundestag_2025_results_summary AS
SELECT
    data->>'Gebiet' as area,
    data->>'Partei' as party,
    (data->>'Stimmen')::INTEGER as votes,
    retrieved_at
FROM bundestag_2025_kerg
WHERE data->>'Gebiet' IS NOT NULL;

CREATE INDEX ON bundestag_2025_results_summary (party);
CREATE INDEX ON bundestag_2025_results_summary (area);

-- Refresh when data updates
REFRESH MATERIALIZED VIEW bundestag_2025_results_summary;
```

## References

- [Official Open Data Documentation](https://www.bundeswahlleiterin.de/bundestagswahlen/2025/ergebnisse/opendata.html)
- [Data Format Specifications (PDF)](https://www.bundeswahlleiterin.de/bundestagswahlen/2025/ergebnisse/opendata/btw25/csv/) (see linked PDF documents)
- [PostgreSQL JSONB Documentation](https://www.postgresql.org/docs/current/datatype-json.html)
