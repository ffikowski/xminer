# Bundestag 2025 Election Results - Quick Start

## 🚀 Quick Setup (3 Steps)

### 1. Setup Database

```bash
psql -U postgres -d your_database -f setup_bundestag_2025_db.sql
```

### 2. Fetch Data

```bash
python -m xminer.tasks.fetch_bundestag_2025_results
```

### 3. Query Data

```sql
-- View first 10 records
SELECT * FROM bundestag_2025_kerg LIMIT 10;

-- View detailed results with percentages
SELECT * FROM bundestag_2025_kerg2 LIMIT 10;
```

## 📊 What You Get

| File | Table | Contains |
|------|-------|----------|
| kerg.csv | `bundestag_2025_kerg` | Absolute vote counts |
| kerg2.csv | `bundestag_2025_kerg2` | Vote counts + percentages + changes |

## 🔍 Common Queries

### View all columns available in the data

```sql
SELECT DISTINCT jsonb_object_keys(data) as column_name
FROM bundestag_2025_kerg
LIMIT 20;
```

### Get results by party

```sql
SELECT
    data->>'Partei' as party,
    COUNT(*) as records,
    SUM((data->>'Stimmen')::INTEGER) as total_votes
FROM bundestag_2025_kerg
WHERE data->>'Partei' IS NOT NULL
GROUP BY data->>'Partei'
ORDER BY total_votes DESC;
```

### Get results by electoral district

```sql
SELECT
    data->>'Wahlkreis' as district,
    data->>'Partei' as party,
    data->>'Stimmen' as votes
FROM bundestag_2025_kerg
WHERE data->>'Wahlkreis' IS NOT NULL
ORDER BY (data->>'Stimmen')::INTEGER DESC
LIMIT 20;
```

## 🔄 Update Data

To get the latest results (as votes are counted):

```bash
python -m xminer.tasks.fetch_bundestag_2025_results
```

## 📍 Data Location

- **CSV files**: `data/bundestag_2025/`
- **Database**: `bundestag_2025_kerg` and `bundestag_2025_kerg2` tables
- **Source**: https://www.bundeswahlleiterin.de/bundestagswahlen/2025/ergebnisse/opendata/btw25/csv/

## 📖 Full Documentation

See [BUNDESTAG_2025_RESULTS.md](docs/BUNDESTAG_2025_RESULTS.md) for:
- Detailed data schema
- Advanced queries
- Integration examples
- Troubleshooting
