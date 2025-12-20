# Xminer Ÿ?" Automated Social Metrics for Political Twitter/X Accounts

Xminer is a Python-based analytics framework that automates the collection and analysis of political activity on **X (Twitter)**.  
It fetches data via the X API, stores it in a Neon-hosted PostgreSQL database, and computes a rich set of monthly and delta (MoM) metrics for accounts, parties, and tweets.

---

## Project Structure

```
xminer/
  src/xminer/
    config/          # Configuration system and parameter loader
    io/              # Input/output connectors
    utils/           # Shared helpers and metric computations
    tasks/           # Executable scripts for fetching & metrics
    pipelines/       # Pipeline orchestration (fetch, metrics, all)
  pyproject.toml
  README.md
```

---

## Configuration

### 1. Environment variables (.env)
Secrets and connection strings:

```
DATABASE_URL=postgresql+psycopg2://user:pass@neon-host/db
X_BEARER_TOKEN=your_x_api_token
```

### 2. Parameters (parameters.yml)
Human-readable configuration file for all runtime options:

```
general:
  file: app.log
  level: INFO

metrics:
  year: 2025
  month: 10
  outdir: output
  top_n: 50

fetch:
  sample_limit: -1
  chunk_size: 100
  load_to_db: true
  store_csv: false

fetch_tweets:
  tweets_sample_limit: -1
  rate_limit_fallback_sleep: 901

trends:
  trends_woeid: 23424829
  trends_place_name: Germany

export:
  ssh_host: 145.223.101.94
  ssh_user: app
  ssh_identity_file: ~/.ssh/id_ed25519
  remote_base_dir: /home/app/apps/xminer/output
  export_patterns:
    - "202510/profiles/*.csv"
    - "202510/tweets/*.csv"
  local_dest_dir: "C:/Users/felix/Documents/xminer/outputs"
```

Parameters can be grouped or flat; the loader automatically resolves both styles.

---

## Running Xminer

### Activate virtual environment
```
.venv\Scripts\activate          # Windows
# or
source .venv/bin/activate       # Linux/macOS
```

### Install dependencies
```
pip install .
# or, for editable installs during development:
pip install -e .
```

### Run single tasks
```
python -m xminer.tasks.fetch_x_profiles
python -m xminer.tasks.tweets_metrics_monthly
```

### Run entire pipelines
The CLI is powered by Typer:

```
python -m xminer.pipelines.cli run fetch     # Fetch profiles + tweets
python -m xminer.pipelines.cli run metrics   # Compute all metrics
python -m xminer.pipelines.cli run all       # Full end-to-end workflow
```
---

## Core Workflows

| Stage | Scripts | Description |
|--------|----------|-------------|
| **1. Fetching** | fetch_x_profiles.py, fetch_tweets.py, fetch_x_trends.py | Collect latest X data for politicians and trending topics. |
| **2. Metrics (monthly)** | x_profile_metrics_monthly.py, tweets_metrics_monthly.py | Compute base metrics for each account and tweet in a given month. |
| **3. Metrics (delta)** | x_profile_metrics_delta.py, tweets_metrics_delta.py | Compute month-over-month growth and change metrics. |
| **4. Export** | export_outputs.py, export_neon.py | Copy generated CSVs from the server or export raw data from the database. |

---

## ÑYä'Ÿ??ÑY'î Development Notes

- SQLAlchemy engine handles Neon DB connections.  
- .env holds secrets; parameters.yml holds runtime configuration.  
- To extend metrics, define new MetricSpecs in utils/metrics_helpers.py.  
- To add new pipelines, extend pipelines/flows.py.
