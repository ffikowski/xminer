# 🧠 Xminer – Automated Social Metrics for Political Twitter/X Accounts

Xminer is a Python-based analytics framework that automates the collection and analysis of political activity on **X (Twitter)**.  
It fetches data via the X API, stores it in a Neon-hosted PostgreSQL database, and computes a rich set of monthly and delta (MoM) metrics for accounts, parties, and tweets.

---

## 📁 Project Structure

```
xminer/
├── src/xminer/
│   ├── config/          # Configuration system and parameter loader
│   ├── io/              # Input/output connectors
│   ├── utils/           # Shared helpers and metric computations
│   ├── tasks/           # Executable scripts for fetching & metrics
│   └── pipelines/       # Pipeline orchestration (fetch, metrics, all)
├── requirements.txt
├── pyproject.toml
└── README.md
```

---

## ⚙️ Configuration

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

## 🚀 Running Xminer

### Activate virtual environment
```
.venv\Scripts\activate          # Windows
# or
source .venv/bin/activate       # Linux/macOS
```

### Install dependencies
```
pip install -r requirements.txt
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

---

## 🧩 Core Workflows

| Stage | Scripts | Description |
|--------|----------|-------------|
| **1. Fetching** | fetch_x_profiles.py, fetch_tweets.py, fetch_x_trends.py | Collect latest X data for politicians and trending topics. |
| **2. Metrics (monthly)** | x_profile_metrics_monthly.py, tweets_metrics_monthly.py | Compute base metrics for each account and tweet in a given month. |
| **3. Metrics (delta)** | x_profile_metrics_delta.py, tweets_metrics_delta.py | Compute month-over-month growth and change metrics. |
| **4. Export** | export_outputs.py, export_neon.py | Copy generated CSVs from the server or export raw data from the database. |

---

## 🧠 Design Philosophy

- Composable tasks: Each script runs standalone.  
- Pipeline orchestration: Defined in pipelines/flows.py and executed via CLI.  
- Configuration-first: All behavior controlled via parameters.yml.  
- Safe testing: `--offline` and `--dry-run` prevent any API or DB side effects.

```

---

## 🖥️ Output Structure

```
output/
└── 202510/
    ├── profiles/
    │   ├── individual_base_202510.csv
    │   ├── party_summary_202510.csv
    │   └── top_accounts_global_202510.csv
    └── tweets/
        ├── tweets_top_by_engagement_rate_202510.csv
        ├── tweets_delta_party_202510.csv
        └── tweets_top_gainers_engagement_202510.csv
```

---

## 🧑‍💻 Development Notes

- SQLAlchemy engine handles Neon DB connections.  
- .env holds secrets; parameters.yml holds runtime configuration.  
- To extend metrics, define new MetricSpecs in utils/metrics_helpers.py.  
- To add new pipelines, extend pipelines/flows.py.
