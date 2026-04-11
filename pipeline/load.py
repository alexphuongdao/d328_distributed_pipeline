"""Initialize and load cleaned data into SQLite."""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.clean import clean_all, save_cleaned_data


LOGGER = logging.getLogger(__name__)


def init_database(db_path: str, schema_path: str) -> None:
    """Create SQLite database and execute schema DDL."""
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = Path(schema_path).read_text(encoding="utf-8")

    with sqlite3.connect(db_file) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(schema_sql)
        conn.commit()


def get_or_create_lookup(
    cursor: sqlite3.Cursor,
    table: str,
    name_col: str,
    value: str,
    extra_cols: dict[str, Any] | None = None,
) -> int:
    """Insert value in lookup table if needed and return integer id."""
    id_col = {
        "incident_types": "type_id",
        "precincts": "precinct_id",
        "sectors": "sector_id",
        "races": "race_id",
        "genders": "gender_id",
    }[table]

    cursor.execute(
        f"SELECT {id_col} FROM {table} WHERE {name_col} = ?",
        (value,),
    )
    found = cursor.fetchone()
    if found:
        return int(found[0])

    extra_cols = extra_cols or {}
    cols = [name_col] + list(extra_cols.keys())
    vals = [value] + list(extra_cols.values())
    placeholders = ", ".join("?" for _ in cols)

    cursor.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders})",
        vals,
    )
    return int(cursor.lastrowid)


def _as_nullable(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value


def upsert_incident(cursor: sqlite3.Cursor, record: dict[str, Any]) -> None:
    """Insert or update one incident row, resolving lookup IDs first."""
    type_id = get_or_create_lookup(
        cursor,
        table="incident_types",
        name_col="type_name",
        value=str(record["incident_type_raw"]),
        extra_cols={"force_level": int(record["force_level"]) if pd.notna(record["force_level"]) else 0},
    )

    precinct_id = (
        get_or_create_lookup(cursor, "precincts", "precinct_name", str(record["precinct"]))
        if pd.notna(record.get("precinct"))
        else None
    )
    sector_id = (
        get_or_create_lookup(cursor, "sectors", "sector_name", str(record["sector"]))
        if pd.notna(record.get("sector"))
        else None
    )
    race_id = (
        get_or_create_lookup(cursor, "races", "race_name", str(record["subject_race"]))
        if pd.notna(record.get("subject_race"))
        else None
    )
    gender_id = (
        get_or_create_lookup(cursor, "genders", "gender_name", str(record["subject_gender"]))
        if pd.notna(record.get("subject_gender"))
        else None
    )

    cursor.execute(
        """
        INSERT INTO incidents (
            uniqueid, incident_num, type_id, incident_date, incident_time,
            incident_year, day_of_week, hour_of_day, precinct_id, sector_id,
            beat, officer_id, subject_id, race_id, gender_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(uniqueid) DO UPDATE SET
            incident_num = excluded.incident_num,
            type_id = excluded.type_id,
            incident_date = excluded.incident_date,
            incident_time = excluded.incident_time,
            incident_year = excluded.incident_year,
            day_of_week = excluded.day_of_week,
            hour_of_day = excluded.hour_of_day,
            precinct_id = excluded.precinct_id,
            sector_id = excluded.sector_id,
            beat = excluded.beat,
            officer_id = excluded.officer_id,
            subject_id = excluded.subject_id,
            race_id = excluded.race_id,
            gender_id = excluded.gender_id,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            str(record["uniqueid"]),
            str(record["incident_num"]),
            type_id,
            _as_nullable(record.get("incident_date")),
            _as_nullable(record.get("incident_time")),
            _as_nullable(record.get("incident_year")),
            _as_nullable(record.get("day_of_week")),
            _as_nullable(record.get("hour_of_day")),
            precinct_id,
            sector_id,
            _as_nullable(record.get("beat")),
            str(record["officer_id"]),
            str(record["subject_id"]),
            race_id,
            gender_id,
        ),
    )


def load_cleaned_data(db_path: str, cleaned_df: pd.DataFrame) -> dict[str, int]:
    """Load cleaned records using transactional upserts."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()

        existing_ids = {
            row[0] for row in cursor.execute("SELECT uniqueid FROM incidents").fetchall()
        }

        total = len(cleaned_df)
        new_count = 0
        updated_count = 0

        for idx, record in enumerate(cleaned_df.to_dict(orient="records"), start=1):
            uniqueid = str(record["uniqueid"])
            if uniqueid in existing_ids:
                updated_count += 1
            else:
                new_count += 1

            upsert_incident(cursor, record)

            if idx % 500 == 0:
                LOGGER.info("Loaded %s/%s records...", idx, total)

        conn.commit()

    return {"total": total, "new": new_count, "updated": updated_count}


def _read_raw_json(raw_path: str) -> pd.DataFrame:
    rows = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    if not isinstance(rows, list):
        raise ValueError("Raw input file must be a JSON list of records.")
    return pd.DataFrame(rows)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Load Seattle UOF data into SQLite")
    parser.add_argument("--raw", default="data/raw/uof_raw.json", help="Raw JSON input path")
    parser.add_argument(
        "--cleaned",
        default="data/cleaned/uof_cleaned.csv",
        help="Cleaned CSV output path",
    )
    parser.add_argument("--db", default="db/seattle_uof.db", help="SQLite DB path")
    parser.add_argument("--schema", default="db/schema.sql", help="SQL schema path")
    return parser


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = _build_arg_parser().parse_args()

    init_database(args.db, args.schema)
    raw_df = _read_raw_json(args.raw)
    cleaned_df = clean_all(raw_df)

    Path(args.cleaned).parent.mkdir(parents=True, exist_ok=True)
    save_cleaned_data(cleaned_df, args.cleaned)

    result = load_cleaned_data(args.db, cleaned_df)
    LOGGER.info("Load complete: %s", result)
