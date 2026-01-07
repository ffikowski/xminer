# Bundestag 2025 Database Schema

## ✅ New Structured Table

### `bundestag_2025_results` (5.2 MB)

Properly structured table with actual columns for easy querying.

**Columns:**
- `wahlart` - Election type (BT = Bundestagswahl)
- `wahltag` - Election date (2025-02-23)
- `gebietsart` - Area type: 'Bund', 'Land', 'Wahlkreis'
- `gebietsnummer` - Area number
- `gebietsname` - Area name
- `gruppenart` - Group type: 'Partei', 'System-Gruppe', 'Einzelbewerber'
- `gruppenname` - Party/group name
- `stimme` - Vote type: 1=Erststimme, 2=Zweitstimme, NULL=system data
- `anzahl` - Vote count (absolute)
- `prozent` - Percentage
- `vorp_anzahl` - Previous election count (2021)
- `vorp_prozent` - Previous percentage (2021)
- `diff_prozent` - Percentage change
- `diff_prozent_pkt` - Percentage point change

**Rows:** 15,617

## 📈 Convenient Views

### `btw2025_bundesergebnis`
Federal election results (Zweitstimme only) with 2021 comparison.

```sql
SELECT * FROM btw2025_bundesergebnis;
```

**Columns:** partei, stimmen, prozent, stimmen_2021, prozent_2021, veraenderung_pkt

### `btw2025_wahlbeteiligung`
Voter turnout by federal state.

```sql
SELECT * FROM btw2025_wahlbeteiligung;
```

**Columns:** bundesland, wahlberechtigte, waehlende, wahlbeteiligung_prozent

### `btw2025_laenderergebnisse`
Party results by federal state (Zweitstimme).

```sql
SELECT * FROM btw2025_laenderergebnisse WHERE bundesland = 'Baden-Württemberg';
```

**Columns:** bundesland, partei, stimmen, prozent, veraenderung_pkt

## 📝 Example Queries

### Get top parties nationwide

```sql
SELECT gruppenname, anzahl, prozent, diff_prozent_pkt
FROM bundestag_2025_results
WHERE gebietsart = 'Bund'
  AND gruppenart = 'Partei'
  AND stimme = 2
ORDER BY anzahl DESC
LIMIT 10;
```

### Compare 2025 vs 2021 results

```sql
SELECT
    gruppenname as party,
    anzahl as votes_2025,
    vorp_anzahl as votes_2021,
    prozent as percent_2025,
    vorp_prozent as percent_2021,
    diff_prozent_pkt as change
FROM bundestag_2025_results
WHERE gebietsart = 'Bund'
  AND gruppenart = 'Partei'
  AND stimme = 2
ORDER BY anzahl DESC;
```

### Get results for specific state

```sql
SELECT
    gruppenname as party,
    anzahl as votes,
    prozent as percent
FROM bundestag_2025_results
WHERE gebietsart = 'Land'
  AND gebietsname = 'Bayern'
  AND gruppenart = 'Partei'
  AND stimme = 2
ORDER BY anzahl DESC;
```

### Voter turnout by state

```sql
SELECT * FROM btw2025_wahlbeteiligung
ORDER BY wahlbeteiligung_prozent DESC;
```

### Find parties with biggest gains

```sql
SELECT
    gruppenname,
    prozent,
    diff_prozent_pkt
FROM bundestag_2025_results
WHERE gebietsart = 'Bund'
  AND gruppenart = 'Partei'
  AND stimme = 2
  AND diff_prozent_pkt IS NOT NULL
ORDER BY diff_prozent_pkt DESC
LIMIT 10;
```

### System data (turnout, invalid votes, etc.)

```sql
SELECT
    gruppenname,
    anzahl,
    prozent
FROM bundestag_2025_results
WHERE gebietsart = 'Bund'
  AND gruppenart = 'System-Gruppe'
  AND stimme IS NULL
ORDER BY gruppenreihenfolge;
```

## 🔗 Joining with Other Tables

### Election results + Politician tweets

```sql
SELECT
    p.vorname,
    p.nachname,
    p.partei_kurz,
    r.anzahl as party_votes,
    r.prozent as party_percent,
    COUNT(t.tweet_id) as tweet_count
FROM politicians_12_2025 p
JOIN bundestag_2025_results r
    ON p.partei_kurz = r.gruppenname
    AND r.gebietsart = 'Bund'
    AND r.gruppenart = 'Partei'
    AND r.stimme = 2
LEFT JOIN tweets t
    ON t.username = p.username
    AND t.created_at >= '2025-01-01'
GROUP BY p.vorname, p.nachname, p.partei_kurz, r.anzahl, r.prozent
ORDER BY r.anzahl DESC;
```

### Election results + Bundestag votes

```sql
SELECT
    r.gruppenname as party,
    r.prozent as election_percent,
    COUNT(DISTINCT v.vote_title) as votes_participated,
    SUM(v.ja) as total_yes_votes
FROM bundestag_2025_results r
LEFT JOIN bundestag_votes v
    ON r.gruppenname = v.fraktion_gruppe
WHERE r.gebietsart = 'Bund'
  AND r.gruppenart = 'Partei'
  AND r.stimme = 2
GROUP BY r.gruppenname, r.prozent
ORDER BY r.prozent DESC;
```

## 🗑️ Old JSONB Tables (deprecated)

These tables still exist but are **not recommended** for use:

- `bundestag_2025_kerg` (4.8 MB) - Old JSONB format
- `bundestag_2025_kerg2` (29 MB) - Old JSONB format

Use `bundestag_2025_results` instead for all queries.

## 📊 2025 Election Summary

- **Election Date:** February 23, 2025
- **Eligible Voters:** 60,510,631
- **Turnout:** 82.51% (49,928,653 voters)
- **Winner:** CDU (22.55%, +3.60 pts from 2021)
- **Second:** AfD (20.80%, +10.42 pts from 2021)
- **Third:** SPD (16.41%, -9.29 pts from 2021)
- **New Parties:** BSW (4.98%), BÜNDNIS DEUTSCHLAND, MERA25, WerteUnion
- **Biggest Gain:** AfD (+10.42 percentage points)
- **Biggest Loss:** SPD (-9.29 percentage points)
