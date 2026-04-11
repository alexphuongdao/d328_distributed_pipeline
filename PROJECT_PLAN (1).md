# Seattle Use of Force — Data Engineering Project

## Project Overview

Build a complete data pipeline for Seattle Police Department's Use of Force (UOF) dataset. The pipeline fetches data from a public REST API, cleans it, loads it into a normalized SQLite database, and serves it through a Streamlit dashboard with batch update capability.

**Data Source:** Seattle Open Data (Socrata SODA2 API)  
**API Endpoint:** `https://data.seattle.gov/resource/ppi5-g2bj.json`  
**Dataset:** Use of Force incidents by SPD officers (~19,800 records, updated daily)  

---

## Project Structure

```
seattle-uof-pipeline/
├── pipeline/
│   ├── __init__.py
│   ├── fetch.py          # Step 1: API data download
│   ├── clean.py          # Step 2: Cleaning functions
│   ├── load.py           # Step 5: DB insertion logic
│   └── update.py         # Step 7: Batch update orchestrator
├── db/
│   ├── schema.sql        # Step 4: Normalized schema DDL
│   └── seattle_uof.db    # Step 5: SQLite database (generated)
├── data/
│   ├── raw/              # Raw JSON from API
│   └── cleaned/          # Cleaned CSV/JSON before DB load
├── app.py                # Step 6: Streamlit dashboard
├── notebooks/
│   └── exploration.ipynb # Optional: EDA and dev scratchpad
├── requirements.txt
└── README.md
```

---

## Raw Data Fields

The API returns JSON records with these 11 fields:

| Field               | Example Values                                           | Issues Found                                              |
|----------------------|----------------------------------------------------------|-----------------------------------------------------------|
| `uniqueid`           | `"2016UOF-0045-1113-7096"`                               | Natural primary key. Appears clean.                       |
| `incident_num`       | `"9240"`, `"57007"`                                      | String numeric. Multiple rows can share same incident_num (one incident, multiple officers/subjects). |
| `incident_type`      | `"Level 1 - Use of Force"`, `"Level 2 - Use of Force"`  | Repeated string, good candidate for lookup table.         |
| `occured_date_time`  | `"2016-01-08T00:13:00.000"`                              | ISO string. Note the typo: "occured" not "occurred". Parse to datetime, split into date + time columns. |
| `precinct`           | `"Southwest"`, `"West"`, `"North"`, `"East"`, `"South"`, `"OOJ"`, `"-"` | `"-"` is a placeholder for missing data. `"OOJ"` = Out of Jurisdiction. |
| `sector`             | `"FRANK"`, `"DAVID"`, `"BOY"`, `"OOJ"`, missing          | NATO phonetic names. Can be missing or `"OOJ"`.           |
| `beat`               | `"F3"`, `"D2"`, `"99"`, `"-"`, missing                   | `"99"` and `"-"` are placeholders. Should be nullable.    |
| `officer_id`         | `"1732"`, `"2230"`                                       | String numeric identifier. NOT unique per row (one officer can appear in multiple incidents). |
| `subject_id`         | `"7048"`, `"742"`                                        | String numeric identifier. NOT unique per row.            |
| `subject_race`       | `"Black or African American"`, `"White"`, `"Hispanic or Latino"`, `"Nat Hawaiian/Oth Pac Islander"`, `"Not Specified"` | Repeated string, good candidate for lookup. Some values are abbreviated inconsistently. |
| `subject_gender`     | `"Male"`, `"Female"`, `"-"`, `"Unknown"`                 | `"-"` is placeholder for missing. Should standardize.     |

---

## Step-by-Step Implementation Plan

### Step 1: Data Pipeline — Fetch from REST API

**File:** `pipeline/fetch.py`

**Function signature:**
```python
def fetch_api_data(api_url: str, output_file: str, batch_size: int = 1000, num_records: int = None) -> str:
    """
    Download data from SODA2 REST API with pagination.

    Uses $limit and $offset query parameters for batched pagination.
    Keeps fetching until fewer than batch_size records are returned
    (meaning we've hit the end) or num_records is reached.

    Args:
        api_url: SODA2 API endpoint URL
        output_file: Path to save the raw JSON output (e.g., "data/raw/uof_raw.json")
        batch_size: Number of records per API request (default 1000, SODA2 max is 50000)
        num_records: Optional cap on total records to fetch. None = fetch all.

    Returns:
        Path to the saved output file.
    """
```

**Implementation notes:**
- Use `requests` library
- Paginate with `$limit` and `$offset` query params: `?$limit=1000&$offset=0`, then `$offset=1000`, etc.
- Stop when a response returns fewer records than `batch_size`
- If `num_records` is set, stop when total fetched >= `num_records`
- Save combined results as a single JSON file to `data/raw/`
- Add logging: `"Fetched 1000 records (offset: 0)..."`
- Handle HTTP errors gracefully (retry with backoff, or at minimum raise informative errors)

---

### Step 2: Cleaning Functions

**File:** `pipeline/clean.py`

Use pandas DataFrames. Each function takes a DataFrame, returns a cleaned DataFrame. One function per concern.

#### Required cleaning functions:

```python
def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert 'occured_date_time' ISO string to proper datetime.
    Add derived columns:
    - 'incident_date' (date only, YYYY-MM-DD)
    - 'incident_time' (time only, HH:MM)
    - 'incident_year' (int, for easy grouping)
    - 'incident_month' (int)
    - 'day_of_week' (string, e.g., "Monday")
    - 'hour_of_day' (int 0-23, for time-of-day analysis)
    Drop or rename the original 'occured_date_time' column.
    """

def clean_precinct(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize precinct values.
    - "-" -> None (null)
    - "OOJ" -> "Out of Jurisdiction"
    - All others: title case (already mostly clean)
    """

def clean_sector(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize sector values.
    - Missing/empty/"-" -> None
    - "OOJ" -> "Out of Jurisdiction"
    - All others: title case the NATO phonetic names ("FRANK" -> "Frank")
    """

def clean_beat(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize beat values.
    - "-" -> None
    - "99" -> None  (this is the placeholder for OOJ/unknown)
    - All others: keep as-is (e.g., "F3", "D2")
    """

def clean_subject_gender(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize subject_gender.
    - "-" -> "Unknown"
    - Keep "Male", "Female", "Unknown" as-is
    - Any other unexpected values -> log a warning and keep original
    """

def clean_subject_race(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize subject_race values.
    - "Nat Hawaiian/Oth Pac Islander" -> "Native Hawaiian/Other Pacific Islander"
    - "Not Specified" -> "Not Specified" (keep as-is, distinct from missing)
    - All others: keep as-is, they are already reasonably clean
    - Null/empty -> "Not Specified"
    """

def clean_incident_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize incident_type.
    - Strip "Level X - " prefix into a separate 'force_level' column (int: 1, 2, or 3)
    - Keep original incident_type as 'incident_type_raw'
    - Create clean 'force_level_label': "Level 1", "Level 2", "Level 3"

    Level reference:
    - Level 1 = lowest (e.g., transient pain, handcuffing)
    - Level 2 = intermediate (e.g., OC spray, Taser)
    - Level 3 = highest (e.g., officer-involved shooting)
    """

def validate_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure uniqueid, incident_num, officer_id, subject_id are strings (not accidentally parsed as ints).
    Verify uniqueid has no nulls (it's our PK).
    Log warning for any duplicate uniqueid values.
    """

def remove_full_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate by uniqueid (the natural key).
    Keep first occurrence. Log how many duplicates were found.
    """
```

#### Master cleaning function:

```python
def clean_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run all cleaning functions in sequence.
    Returns cleaned DataFrame.
    """
    df = validate_ids(df)
    df = parse_dates(df)
    df = clean_precinct(df)
    df = clean_sector(df)
    df = clean_beat(df)
    df = clean_subject_gender(df)
    df = clean_subject_race(df)
    df = clean_incident_type(df)
    df = remove_full_duplicates(df)
    return df
```

---

### Step 3: Save Cleaned Data

After running `clean_all()`, save the output to `data/cleaned/uof_cleaned.csv`. This gives you a checkpoint before database loading.

```python
def save_cleaned_data(df: pd.DataFrame, output_path: str) -> str:
    """Save cleaned DataFrame to CSV. Returns the output path."""
```

---

### Step 4: Database Schema (Normalized)

**File:** `db/schema.sql`

The goal: eliminate redundancy, enforce integrity, simplify querying. Repeated strings (race, gender, incident type, precinct, sector) become lookup tables.

```sql
-- Lookup Tables

CREATE TABLE IF NOT EXISTS incident_types (
    type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    type_name TEXT NOT NULL UNIQUE,            -- "Level 1 - Use of Force", etc.
    force_level INTEGER NOT NULL               -- 1, 2, or 3
);

CREATE TABLE IF NOT EXISTS precincts (
    precinct_id INTEGER PRIMARY KEY AUTOINCREMENT,
    precinct_name TEXT NOT NULL UNIQUE          -- "North", "South", "East", "West", "Southwest", "Out of Jurisdiction"
);

CREATE TABLE IF NOT EXISTS sectors (
    sector_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_name TEXT NOT NULL UNIQUE            -- "Frank", "David", "Boy", etc.
);

CREATE TABLE IF NOT EXISTS races (
    race_id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_name TEXT NOT NULL UNIQUE              -- "White", "Black or African American", etc.
);

CREATE TABLE IF NOT EXISTS genders (
    gender_id INTEGER PRIMARY KEY AUTOINCREMENT,
    gender_name TEXT NOT NULL UNIQUE            -- "Male", "Female", "Unknown"
);

-- Main Fact Table

CREATE TABLE IF NOT EXISTS incidents (
    uniqueid TEXT PRIMARY KEY,                 -- natural key from API
    incident_num TEXT NOT NULL,                -- groups multiple rows (same incident, diff officers/subjects)
    type_id INTEGER NOT NULL REFERENCES incident_types(type_id),
    incident_date DATE,
    incident_time TEXT,                        -- stored as "HH:MM" string
    incident_year INTEGER,
    day_of_week TEXT,
    hour_of_day INTEGER,
    precinct_id INTEGER REFERENCES precincts(precinct_id),
    sector_id INTEGER REFERENCES sectors(sector_id),
    beat TEXT,                                 -- kept as-is (too many unique values for lookup)
    officer_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    race_id INTEGER REFERENCES races(race_id),
    gender_id INTEGER REFERENCES genders(gender_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common dashboard queries
CREATE INDEX IF NOT EXISTS idx_incidents_year ON incidents(incident_year);
CREATE INDEX IF NOT EXISTS idx_incidents_date ON incidents(incident_date);
CREATE INDEX IF NOT EXISTS idx_incidents_precinct ON incidents(precinct_id);
CREATE INDEX IF NOT EXISTS idx_incidents_type ON incidents(type_id);
CREATE INDEX IF NOT EXISTS idx_incidents_race ON incidents(race_id);
CREATE INDEX IF NOT EXISTS idx_incidents_officer ON incidents(officer_id);
CREATE INDEX IF NOT EXISTS idx_incidents_incident_num ON incidents(incident_num);

-- Update log for batch tracking (Step 7)
CREATE TABLE IF NOT EXISTS update_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    records_fetched INTEGER,
    records_new INTEGER,
    records_updated INTEGER,
    errors TEXT,
    status TEXT  -- 'success' or 'failed'
);
```

**Why this schema:**
- **Lookup tables** for incident_type, precinct, sector, race, gender → no repeated strings in main table, easy filtering, referential integrity
- **beat stays in main table** → too many unique short values with low repetition, not worth a lookup
- **uniqueid as PK** → natural key from the API, enables upsert for batch updates
- **incident_num is NOT unique** → one incident can involve multiple officer-subject pairs, so it's indexed but not a key
- **Derived time columns** (year, hour_of_day, day_of_week) → pre-computed for fast dashboard queries
- **created_at / updated_at** → tracks when records entered DB and last refresh

---

### Step 5: Create Database and Load Data

**File:** `pipeline/load.py`

```python
def init_database(db_path: str, schema_path: str):
    """Create the SQLite database and execute schema.sql to build all tables."""

def get_or_create_lookup(cursor, table: str, name_col: str, value: str, extra_cols: dict = None) -> int:
    """
    Generic helper: insert into lookup table if not exists, return the ID.
    Works for any lookup table (incident_types, precincts, sectors, races, genders).

    Args:
        cursor: sqlite3 cursor
        table: table name, e.g. "precincts"
        name_col: the text column name, e.g. "precinct_name"
        value: the value to insert/find, e.g. "North"
        extra_cols: optional dict of additional columns, e.g. {"force_level": 1}

    Returns:
        The integer ID of the row.
    """

def upsert_incident(cursor, record: dict):
    """
    Insert or update a single incident record.
    Uses INSERT ... ON CONFLICT(uniqueid) DO UPDATE for upsert.
    Resolves all foreign keys via get_or_create_lookup.
    """

def load_cleaned_data(db_path: str, cleaned_df: pd.DataFrame) -> dict:
    """
    Load all cleaned records into the database.
    Wraps everything in a transaction for atomicity.
    Logs progress every 500 records.

    Returns:
        dict with counts: {"total": N, "new": N, "updated": N}
    """
```

---

### Step 6: Streamlit Dashboard

**File:** `app.py`

**Layout plan:**

```
┌──────────────────────────────────────────────────────┐
│  Seattle Police Use of Force Dashboard               │
├──────────────────────────────────────────────────────┤
│                                                      │
│  SIDEBAR:                                            │
│  ┌──────────────────┐                                │
│  │ Filters          │   MAIN AREA:                   │
│  │ - Year range     │                                │
│  │ - Force level    │   Tab 1: Overview              │
│  │ - Precinct       │   - KPI cards: total incidents,│
│  │ - Subject race   │     Level 1/2/3 counts         │
│  │ - Subject gender │   - Incidents by year (bar)    │
│  │                  │   - By force level (pie/donut) │
│  │ [Refresh Data]   │   - By precinct (bar)          │
│  │ Last updated:    │   - By hour of day (line)      │
│  │ 2026-03-17 06:00 │   - By day of week (bar)       │
│  └──────────────────┘                                │
│                                                      │
│                      Tab 2: Demographics             │
│                      - By subject race (bar)         │
│                      - Race x force level (stacked)  │
│                      - By subject gender (bar)       │
│                      - Gender x force level           │
│                                                      │
│                      Tab 3: Data Explorer            │
│                      - Filterable table with all     │
│                        records (joined with lookups) │
│                      - Download as CSV button        │
│                                                      │
│                      Tab 4: SQL Query                │
│                      - Text area for custom SQL      │
│                      - Execute (read-only enforced)  │
│                      - Results table                 │
│                                                      │
│                      Tab 5: Update Log               │
│                      - Last refresh timestamp        │
│                      - Records added/updated         │
│                      - History of all batch runs     │
│                                                      │
└──────────────────────────────────────────────────────┘
```

**Key implementation details:**

- Use `st.cache_resource` for the DB connection
- Use `st.cache_data(ttl=300)` for query results so they refresh after an update
- SQL Query tab must enforce READ-ONLY: open DB with `sqlite3.connect("file:db/seattle_uof.db?mode=ro", uri=True)`
- Charts: use `plotly.express` consistently via `st.plotly_chart()`
- "Refresh Data" button triggers `pipeline.update.run_batch_update()` with `st.spinner()` and success/error toast
- Provide a useful default query in the SQL tab, e.g.:
  ```sql
  SELECT i.incident_date, it.type_name, p.precinct_name, r.race_name, g.gender_name
  FROM incidents i
  JOIN incident_types it ON i.type_id = it.type_id
  LEFT JOIN precincts p ON i.precinct_id = p.precinct_id
  LEFT JOIN races r ON i.race_id = r.race_id
  LEFT JOIN genders g ON i.gender_id = g.gender_id
  ORDER BY i.incident_date DESC
  LIMIT 100;
  ```
- IMPORTANT counting note: "Number of incidents" should use `COUNT(DISTINCT incident_num)`, not `COUNT(*)`, because one incident can have multiple rows.

---

### Step 7: Batch Update Mechanism

**File:** `pipeline/update.py`

```python
def run_batch_update(db_path: str, api_url: str) -> dict:
    """
    Full batch update pipeline:

    1. Fetch latest data from API (fetch_api_data)
    2. Clean it (clean_all)
    3. Upsert into database (load_cleaned_data)
    4. Log the update to update_log table

    Returns:
        dict: {
            "timestamp": str,
            "total_fetched": int,
            "new_records": int,
            "updated_records": int,
            "errors": str or None,
            "status": "success" or "failed"
        }
    """
```

**Update strategy:**
- Fetch ALL records from API each run (~19k records, small enough to pull entirely)
- Use `INSERT ... ON CONFLICT(uniqueid) DO UPDATE` for upsert
- Count new vs updated by checking which uniqueids didn't exist before the upsert
- Log each run to the `update_log` table

**Triggering (implement primary, mention others in presentation):**
- **Primary:** "Refresh Data" button in Streamlit dashboard (demo-friendly)
- **Secondary:** Run `python -m pipeline.update` from command line
- **Production concept:** Cron job (`0 6 * * *`) — mention in presentation

**Also make `pipeline/update.py` runnable standalone:**
```python
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run batch update for Seattle UOF data")
    parser.add_argument("--db", default="db/seattle_uof.db", help="Path to SQLite database")
    parser.add_argument("--api", default="https://data.seattle.gov/resource/ppi5-g2bj.json", help="API URL")
    args = parser.parse_args()
    result = run_batch_update(args.db, args.api)
    print(f"Update complete: {result}")
```

---

### Step 8: Presentation

**Suggested slide outline:**

1. **Title slide** — project name, your name, date
2. **Dataset overview** — what the data is (SPD Use of Force), why it matters (police accountability/transparency), source, size (~19k records, updated daily)
3. **Architecture diagram** — pipeline flow: `SODA2 API -> Fetch (paginated) -> Clean (9 functions) -> SQLite (normalized) -> Streamlit Dashboard`
4. **Data quality issues found** — show real examples: `"-"` as missing values, `"Nat Hawaiian/Oth Pac Islander"` abbreviation, `"99"` beats, `"occured"` typo in field name, one incident having multiple rows
5. **Cleaning approach** — list the functions with before/after examples
6. **Database design** — show the ER diagram, explain normalization: why lookup tables for race, gender, precinct, incident_type; why beat stays in main table
7. **Dashboard demo** — live walkthrough of each tab (Overview -> Demographics -> Data Explorer -> SQL Query)
8. **Batch update demo** — click "Refresh Data", show spinner, show Update Log tab with new entry
9. **Lessons learned / challenges**
10. **Q&A**

---

## Dependencies

**requirements.txt:**
```
requests
pandas
streamlit
plotly
```

No other dependencies needed. `sqlite3` is part of Python's standard library.

---

## Run Commands

```bash
# Initial setup: fetch, clean, and load into database
python -m pipeline.fetch
python -m pipeline.load

# Launch dashboard
streamlit run app.py

# Manual batch update (alternative to dashboard button)
python -m pipeline.update
```

---

## Agent Implementation Notes

- Use pandas DataFrames for all cleaning functions — input a DataFrame, return a DataFrame
- Every cleaning function should be independently testable
- Use `logging` module throughout, not `print()`
- The SQLite DB file goes in `db/seattle_uof.db` — add `db/*.db` to `.gitignore`
- Raw and cleaned data files go in `data/` — add `data/raw/*.json` and `data/cleaned/*.csv` to `.gitignore`
- For the Streamlit SQL query tab, open a SEPARATE read-only connection: `sqlite3.connect("file:db/seattle_uof.db?mode=ro", uri=True)`
- Be mindful that `incident_num` is NOT unique — one incident can have multiple rows (different officer/subject combinations). This is important for accurate counting in the dashboard. "Number of incidents" should be `COUNT(DISTINCT incident_num)`, not `COUNT(*)`.
- The `uniqueid` encodes year, incident number, officer, and subject — it IS the true row-level unique key.
