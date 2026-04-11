"""Fetch raw Seattle UOF data from Socrata SODA2 API."""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)


def fetch_api_data(
    api_url: str,
    output_file: str,
    batch_size: int = 1000,
    num_records: int | None = None,
) -> str:
    """Download data from SODA2 REST API with pagination."""
    records: list[dict[str, Any]] = []
    offset = 0
    session = requests.Session()

    while True:
        params = {"$limit": batch_size, "$offset": offset}
        retries = 3

        for attempt in range(1, retries + 1):
            try:
                response = session.get(api_url, params=params, timeout=30)
                response.raise_for_status()
                batch = response.json()
                if not isinstance(batch, list):
                    raise ValueError("API response is not a list of records.")
                break
            except (requests.RequestException, ValueError) as exc:
                if attempt == retries:
                    raise RuntimeError(
                        f"Failed to fetch API data at offset {offset}: {exc}"
                    ) from exc
                sleep_s = 2**attempt
                LOGGER.warning(
                    "Fetch failed at offset %s (attempt %s/%s). Retrying in %ss...",
                    offset,
                    attempt,
                    retries,
                    sleep_s,
                )
                time.sleep(sleep_s)

        fetched_count = len(batch)
        records.extend(batch)
        LOGGER.info("Fetched %s records (offset: %s)...", fetched_count, offset)

        if num_records is not None and len(records) >= num_records:
            records = records[:num_records]
            LOGGER.info("Reached requested record cap: %s", num_records)
            break

        if fetched_count < batch_size:
            LOGGER.info("Reached end of dataset at offset %s.", offset)
            break

        offset += batch_size

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(records, indent=2), encoding="utf-8")
    LOGGER.info("Saved %s records to %s", len(records), output_path)
    return str(output_path)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Seattle UOF data from API")
    parser.add_argument(
        "--api",
        default="https://data.seattle.gov/resource/ppi5-g2bj.json",
        help="SODA2 API endpoint",
    )
    parser.add_argument(
        "--output",
        default="data/raw/uof_raw.json",
        help="Path to write fetched raw JSON",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Records per request",
    )
    parser.add_argument(
        "--num-records",
        type=int,
        default=None,
        help="Optional cap on total records",
    )
    return parser


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    args = _build_arg_parser().parse_args()
    fetch_api_data(
        api_url=args.api,
        output_file=args.output,
        batch_size=args.batch_size,
        num_records=args.num_records,
    )
