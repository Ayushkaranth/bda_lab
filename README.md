# E-Commerce Big Data Analytics Platform

A Big Data Analytics mini-project that runs entirely on a laptop with free, open-source tools. No cloud account, no credit card, no internet needed after installation.

**Raw CSV + JSON (~800 MB, 8.2M records) → Bronze → Silver (partitioned Parquet) → Gold (DuckDB) → ML + association rules + MapReduce → Streamlit dashboard**

| Concept | Where |
|---|---|
| Data lake + medallion architecture (Bronze / Silver / Gold) | `src/pipeline.py` |
| Columnar storage (Parquet + ZSTD, ~12× smaller than raw) and year/month partitioning | `src/pipeline.py` |
| Data-quality checks (duplicates, bad dates, misspelt cities, text prices, invalid rows) | `src/pipeline.py` → Data Pipeline page |
| Semi-structured data (nested JSON clickstream) | `src/generate_data.py`, `src/pipeline.py` |
| MapReduce from scratch (input splits, map, combine, shuffle, reduce) | `src/mapreduce.py` → MapReduce Demo page |
| Customer segmentation: RFM + K-Means, elbow method, silhouette score | `src/pipeline.py` → Customer Segments page |
| Market basket analysis: support, confidence, lift | `src/pipeline.py` → Market Basket page |
| OLAP analytics with DuckDB, star schema | Gold layer → Overview, Sales, Products, SQL Console |

## Getting started

### Requirements

- **Python 3.10 or newer** ([python.org/downloads](https://www.python.org/downloads/)). On Windows, tick **"Add Python to PATH"** during installation.
- About **2 GB of free disk space** (generated data + libraries).
- Any laptop with 8 GB RAM. 4 GB works with the smaller dataset option below.

### Option A: one click (Windows)

1. Download this repository (green **Code** button → **Download ZIP**, then extract) or clone it:
   ```bash
   git clone https://github.com/Ayushkaranth/bda_lab.git
   cd bda_lab
   ```
2. Double-click **`setup_and_run.bat`**.

It creates a virtual environment, installs the libraries, generates the data, runs the pipeline and opens the dashboard at **http://localhost:8501**. The first run takes 1–3 minutes, depending mostly on download speed.

After that, double-click **`start_dashboard.bat`** to open the dashboard again without rebuilding anything.

### Option B: step by step (Windows, macOS, Linux)

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate            # Windows (cmd)
# .\.venv\Scripts\Activate.ps1    # Windows (PowerShell)
# source .venv/bin/activate       # macOS / Linux

# 2. Install the libraries
pip install -r requirements.txt

# 3. Generate the source data   (~10 s  -> data/raw/)
python -m src.generate_data

# 4. Run the pipeline           (~30 s  -> data/lake/ and data/warehouse.duckdb)
python -m src.pipeline

# 5. Open the dashboard         (http://localhost:8501)
streamlit run app.py
```

Run all commands from the project root folder, the one that contains `app.py`.

### Smaller dataset (slow or low-memory computers)

```bash
python -m src.generate_data --orders 500000 --customers 100000 --sessions 150000
python -m src.pipeline
```

### Run MapReduce on its own

```bash
python -m src.mapreduce --workers 8 --splits 32     # prints a JSON report
```

## Dashboard pages

| Page | What it shows |
|---|---|
| Overview | KPIs, monthly revenue, revenue by category and region (with filters) |
| Sales Trends | Daily revenue + 7-day moving average, weekday × hour heatmap, payment methods, cities |
| Products | Top products, category treemap, seasonality heatmap, rating vs revenue |
| Customer Segments | RFM + K-Means segments, elbow chart, spend share, suggested actions |
| Market Basket | Association rules with lift/confidence sliders, "frequently bought together" recommender |
| Clickstream Funnel | View → cart → purchase funnel, conversion by device |
| Data Pipeline | Architecture, step timings, storage per layer, data-quality report, partition pruning |
| MapReduce Demo | Run the MapReduce job live with any number of workers; result checked against SQL |
| SQL Console | Run your own DuckDB SQL on the warehouse (read-only) |

## Project layout

```
├── app.py                 Streamlit dashboard (9 pages)
├── config.py              paths
├── requirements.txt       duckdb, pandas, numpy, pyarrow, scikit-learn, streamlit, plotly
├── setup_and_run.bat      one-click setup + run (Windows)
├── start_dashboard.bat    open the dashboard only (Windows)
└── src/
    ├── catalog.py         cities, product catalogue, "bought together" pairs
    ├── generate_data.py   creates messy source data (CSV + nested JSON)
    ├── pipeline.py        Bronze → Silver → Gold, data quality, K-Means, association rules, MapReduce timings
    └── mapreduce.py       MapReduce from scratch over the raw CSV
```

`data/` is created when you run the project and is not stored in Git (it is ~1 GB).

## Troubleshooting

| Problem | Fix |
|---|---|
| `python` is not recognised | Reinstall Python and tick "Add Python to PATH", or use `py` instead of `python` |
| `streamlit` is not recognised | Activate the virtual environment first (step 1) |
| PowerShell refuses to run `Activate.ps1` | `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`, or use the `.bat` files |
| Dashboard says "No warehouse found" | Run steps 3 and 4 |
| Pipeline fails because the warehouse file is in use | Stop the dashboard (Ctrl + C), run the pipeline, then start the dashboard again |
| Strange characters in the terminal on Windows | `set PYTHONIOENCODING=utf-8` (the `.bat` files already do this) |

## Tech stack

Python · NumPy · Pandas · DuckDB · Apache Parquet (PyArrow) · multiprocessing · scikit-learn · Streamlit · Plotly. All free and open source.

> The dataset is synthetic (generated by `src/generate_data.py` with a fixed seed), so results are reproducible and there are no privacy issues.
