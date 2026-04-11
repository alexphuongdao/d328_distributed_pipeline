# Seattle UOF Pipeline

End-to-end data engineering project for Seattle Police Department Use of Force data.

## What It Does

- Fetches records from the Seattle Open Data API (SODA2).
- Cleans and standardizes the dataset.
- Loads data into a normalized SQLite database.
- Serves analytics in a Streamlit dashboard.
- Supports repeatable batch updates with update logging.

## Project Layout

```
.
├── app.py
├── db/
│   ├── schema.sql
│   └── seattle_uof.db
├── data/
│   ├── cleaned/
│   └── raw/
├── pipeline/
│   ├── __init__.py
│   ├── clean.py
│   ├── fetch.py
│   ├── load.py
│   └── update.py
├── requirements.txt
└── README.md
```

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
# Fetch raw data
python -m pipeline.fetch

# Clean + load into SQLite
python -m pipeline.load

# Start dashboard
streamlit run app.py

# Run manual batch update
python -m pipeline.update
```
