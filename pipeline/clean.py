"""Cleaning utilities for Seattle UOF records."""

from __future__ import annotations

import logging
import re

import pandas as pd


LOGGER = logging.getLogger(__name__)


def parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Convert occured_date_time to normalized date/time analysis columns."""
    out = df.copy()
    dt = pd.to_datetime(out.get("occured_date_time"), errors="coerce")
    out["incident_date"] = dt.dt.date.astype("string")
    out["incident_time"] = dt.dt.strftime("%H:%M")
    out["incident_year"] = dt.dt.year.astype("Int64")
    out["incident_month"] = dt.dt.month.astype("Int64")
    out["day_of_week"] = dt.dt.day_name()
    out["hour_of_day"] = dt.dt.hour.astype("Int64")
    if "occured_date_time" in out.columns:
        out = out.drop(columns=["occured_date_time"])
    return out


def clean_precinct(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize precinct values."""
    out = df.copy()
    val = out["precinct"].astype("string").str.strip()
    val = val.replace({"-": pd.NA, "": pd.NA, "OOJ": "Out of Jurisdiction"})
    out["precinct"] = val.str.title()
    out.loc[out["precinct"].str.lower() == "out of jurisdiction", "precinct"] = (
        "Out of Jurisdiction"
    )
    return out


def clean_sector(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize sector values."""
    out = df.copy()
    val = out["sector"].astype("string").str.strip()
    val = val.replace({"-": pd.NA, "": pd.NA, "OOJ": "Out of Jurisdiction"})
    out["sector"] = val.str.title()
    out.loc[out["sector"].str.lower() == "out of jurisdiction", "sector"] = (
        "Out of Jurisdiction"
    )
    return out


def clean_beat(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize beat values and remove placeholders."""
    out = df.copy()
    val = out["beat"].astype("string").str.strip()
    val = val.replace({"-": pd.NA, "99": pd.NA, "": pd.NA})
    out["beat"] = val
    return out


def clean_subject_gender(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize subject gender values and warn for unknown categories."""
    out = df.copy()
    val = out["subject_gender"].astype("string").str.strip()
    val = val.replace({"-": "Unknown", "": "Unknown"})
    valid = {"Male", "Female", "Unknown"}
    unknown_values = sorted(set(v for v in val.dropna().unique() if v not in valid))
    if unknown_values:
        LOGGER.warning("Unexpected subject_gender values found: %s", unknown_values)
    out["subject_gender"] = val
    return out


def clean_subject_race(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize race values, expanding known abbreviation and filling missing."""
    out = df.copy()
    val = out["subject_race"].astype("string").str.strip()
    val = val.replace(
        {
            "": pd.NA,
            "Nat Hawaiian/Oth Pac Islander": "Native Hawaiian/Other Pacific Islander",
        }
    )
    out["subject_race"] = val.fillna("Not Specified")
    return out


def clean_incident_type(df: pd.DataFrame) -> pd.DataFrame:
    """Extract force level metadata from incident_type string."""
    out = df.copy()
    out["incident_type_raw"] = out["incident_type"].astype("string").str.strip()

    def _parse_force_level(value: str) -> int | None:
        if value is None or pd.isna(value):
            return None
        match = re.search(r"Level\s*(\d+)", str(value), flags=re.IGNORECASE)
        if not match:
            return None
        return int(match.group(1))

    out["force_level"] = out["incident_type_raw"].apply(_parse_force_level).astype("Int64")
    out["force_level_label"] = out["force_level"].apply(
        lambda x: f"Level {int(x)}" if pd.notna(x) else pd.NA
    )
    return out


def validate_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure core IDs are strings and validate uniqueness assumptions."""
    out = df.copy()
    id_cols = ["uniqueid", "incident_num", "officer_id", "subject_id"]
    for col in id_cols:
        if col in out.columns:
            out[col] = out[col].astype("string")

    null_uniqueid = int(out["uniqueid"].isna().sum())
    if null_uniqueid:
        raise ValueError(f"Found {null_uniqueid} null uniqueid values.")

    dup_count = int(out.duplicated(subset=["uniqueid"]).sum())
    if dup_count:
        LOGGER.warning("Found %s duplicate uniqueid values.", dup_count)
    return out


def remove_full_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate by natural key uniqueid and log removal count."""
    before = len(df)
    out = df.drop_duplicates(subset=["uniqueid"], keep="first").copy()
    removed = before - len(out)
    if removed:
        LOGGER.info("Removed %s duplicate rows by uniqueid.", removed)
    return out


def clean_all(df: pd.DataFrame) -> pd.DataFrame:
    """Run all cleaning functions in sequence."""
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


def save_cleaned_data(df: pd.DataFrame, output_path: str) -> str:
    """Save cleaned DataFrame to CSV and return output path."""
    path = pd.io.common.stringify_path(output_path)
    pd.DataFrame(df).to_csv(path, index=False)
    return path
