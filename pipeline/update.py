"""Batch update orchestration for Seattle UOF pipeline."""

from __future__ import annotations

import argparse
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from pipeline.clean import clean_all, save_cleaned_data
from pipeline.fetch import fetch_api_data
from pipeline.load import init_database, load_cleaned_data


LOGGER = logging.getLogger(__name__)


def _log_update(
    db_path: str,
    records_fetched: int,
    records_new: int,
    records_updated: int,
    errors: str | None,
    status: str,
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO update_log (records_fetched, records_new, records_updated, errors, status)
            VALUES (?, ?, ?, ?, ?)
            """,
            (records_fetched, records_new, records_updated, errors, status),
        )
        conn.commit()


def run_batch_update(db_path: str, api_url: str) -> dict:
    """Fetch, clean, upsert, and log a full batch update run."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    result = {
        "timestamp": timestamp,
        "total_fetched": 0,
        "new_records": 0,
        "updated_records": 0,
        "errors": None,
        "status": "failed",
    }

    try:
        init_database(db_path=db_path, schema_path="db/schema.sql")

        raw_path = fetch_api_data(
            api_url=api_url,
            output_file="data/raw/uof_raw.json",
            batch_size=1000,
            num_records=None,
        )
        raw_df = pd.read_json(raw_path)
        cleaned_df = clean_all(raw_df)
        Path("data/cleaned").mkdir(parents=True, exist_ok=True)
        save_cleaned_data(cleaned_df, "data/cleaned/uof_cleaned.csv")

        load_counts = load_cleaned_data(db_path, cleaned_df)

        result["total_fetched"] = load_counts["total"]
        result["new_records"] = load_counts["new"]
        result["updated_records"] = load_counts["updated"]
        result["status"] = "success"
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("Batch update failed")
        result["errors"] = str(exc)

    _log_update(
        db_path=db_path,
        records_fetched=result["total_fetched"],
        records_new=result["new_records"],
        records_updated=result["updated_records"],
        errors=result["errors"],
        status=result["status"],
    )
    return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    parser = argparse.ArgumentParser(description="Run batch update for Seattle UOF data")
    parser.add_argument("--db", default="db/seattle_uof.db", help="Path to SQLite database")
    parser.add_argument(
        "--api",
        default="https://data.seattle.gov/resource/ppi5-g2bj.json",
        help="API URL",
    )
    args = parser.parse_args()
    update_result = run_batch_update(args.db, args.api)
    print(f"Update complete: {update_result}")
