# pyright: reportMissingImports=false, reportMissingModuleSource=false
"""Thin CLI orchestrator for the BusinessWatcherBot pipeline.

Stages are implemented in pipeline.py:
  1. Fetch    - download raw CSV text from Cabinet Office URLs
  2. Transform - normalise and clean each source into a DataFrame
  3. Validate  - Pydantic schema validation (via validation.py)
  4. Persist   - write CSV snapshots and optionally upsert to Postgres

Usage:
  python fetcher.py 20260309
  python fetcher.py 20260309 --dry_run=1
  python fetcher.py 20260101,20260208
"""

import argparse
import csv
import os
from datetime import datetime
from logging import DEBUG, StreamHandler, getLogger
from os.path import dirname, join
from typing import Dict, List, Optional
from urllib.parse import urlparse

from dotenv import load_dotenv
from sqlalchemy import create_engine

from fetcher_shared import (
    SOURCE_CONFIG,
    build_source_urls,
    can_resolve_host,
    decode_csv_bytes,
    enforce_supabase_ssl,
    http_get,
    normalize_database_url,
    parse_truthy,
)
from pipeline import assemble_dataframes, fetch_sources, persist_csv, persist_db
from validation import validate_dataframe

# Alias kept for backwards compatibility with tests and external callers
CSV_SOURCE_CONFIG = SOURCE_CONFIG

logger = getLogger(__name__)
if not logger.handlers:
    handler = StreamHandler()
    handler.setLevel(DEBUG)
    logger.addHandler(handler)
logger.setLevel(DEBUG)
logger.propagate = False


# ---------------------------------------------------------------------------
# Thin wrappers - preserve public names used by test_debug.py and callers
# ---------------------------------------------------------------------------

def _truthy(value: Optional[str]) -> bool:
    return parse_truthy(value)


def _decode_csv_bytes(content: bytes) -> str:
    return decode_csv_bytes(content, logger)


def _get(url: str) -> Optional[object]:
    return http_get(url, logger)


def _enforce_supabase_ssl(db_url: str) -> str:
    return enforce_supabase_ssl(db_url)


def _can_resolve_host(db_url: str) -> bool:
    return can_resolve_host(db_url, logger)


def construct_urls(today: str) -> List[Dict[str, str]]:
    return build_source_urls(today, CSV_SOURCE_CONFIG)


def construct_path() -> List[Dict[str, str]]:
    return [
        {'pattern': e['pattern'], 'header_skip': e['header_skip'], 'region': e['region']}
        for e in CSV_SOURCE_CONFIG
    ]


def retrieve_csv_file(url: str) -> Optional[str]:
    res = _get(url)
    if res is None or res.status_code != 200:  # type: ignore[union-attr]
        return None
    return _decode_csv_bytes(res.content)  # type: ignore[union-attr]


def create_file_name(url: str) -> str:
    url_parts = url.split('/')
    date = url_parts[-3:-1]
    number = url_parts[-1].replace('watcher', '').replace('.csv', '')
    return '.'.join(date + [number]) + '.csv'


def download_csv(url: str) -> Optional[str]:
    res = _get(url)
    if res is None or res.status_code != 200:  # type: ignore[union-attr]
        return None
    data = _decode_csv_bytes(res.content)  # type: ignore[union-attr]
    file_name = create_file_name(url)
    with open(file_name, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        for row in csv.reader(data.splitlines()):
            writer.writerow(row)
    return file_name


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def insert_data(
    target_date: str,
    dry_run: bool = False,
    MANUAL_RUN: bool = True,
    *,
    file_path_tpl: str = './%s_%s.csv',
    db_url: str = '',
) -> None:
    """Run the 4-stage pipeline for target_date (format: YYYYMMDD)."""
    if MANUAL_RUN:
        today_dt = datetime.strptime(target_date, '%Y%m%d')
        today = today_dt.strftime('%Y-%m-%d')
    else:
        today_dt = datetime.today()
        today = today_dt.strftime('%Y-%m-%d')

    # Stage 1: Fetch
    fetched = fetch_sources(today, CSV_SOURCE_CONFIG, logger)

    # Stage 2: Transform
    append_df = assemble_dataframes(fetched, today_dt, logger)
    if append_df is None:
        logger.warning(f"No dataframes were constructed for {today}. Nothing to save or insert.")
        return

    # Stage 3: Validate
    try:
        valid_df, invalid_df, errors = validate_dataframe(append_df)
        valid_count = len(valid_df) if valid_df is not None else 0
        invalid_count = len(invalid_df) if invalid_df is not None else 0
        error_count = len(errors) if errors is not None else 0
        logger.info(
            f"Validation summary - valid: {valid_count}, invalid: {invalid_count}, errors: {error_count}"
        )
        if invalid_df is not None and not invalid_df.empty:
            logger.warning(f"Found {invalid_count} invalid rows; they will be skipped from DB insert.")
    except Exception as exc:
        logger.error(f"Validation failed before insert: {exc}")
        return

    # Stage 4a: Persist CSV
    persist_csv(append_df, valid_df, invalid_df, errors, file_path_tpl, today, logger)

    if dry_run:
        logger.info(
            f"Dry run: skipping database upsert. CSV outputs and error logs have been written. "
            f"Counts - valid: {valid_count}, invalid: {invalid_count}, errors logged: {error_count}"
        )
        return

    # Stage 4b: Persist DB
    try:
        safe_db_url = _enforce_supabase_ssl(db_url)
        if not _can_resolve_host(safe_db_url):
            logger.error(
                "Cannot resolve database host. Check your internet/DNS settings or the DATABASE_URL hostname."
            )
            return
        engine = create_engine(safe_db_url)
        if valid_df is not None and not valid_df.empty:
            upsert_mode = os.environ.get('UPSERT_MODE', 'nothing').lower()
            attempted = persist_db(
                engine, valid_df, os.environ['BUSINESS_WATCHER_BOT_TABLE_NAME'], upsert_mode
            )
            logger.debug(
                f'validated records as of {today} attempted to upsert: {attempted} (mode={upsert_mode})'
            )
        else:
            logger.warning('No valid rows to insert after validation.')
    except Exception as exc:
        logger.error(f'error on insert (upsert): {exc}')


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Arguments of report downloader')
    parser.add_argument('target_dates', help='Set a target date to download data')
    parser.add_argument('--dry_run', help='set true if dry run (accepts 1/true/yes)')
    args = parser.parse_args()

    try:
        load_dotenv(join(dirname(__file__), '.env'))
    except Exception:
        pass

    OUTPUT_DIR = os.environ.get('OUTPUT_DIR', './')
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    except Exception:
        logger.warning(f"Could not create output directory '{OUTPUT_DIR}'. Falling back to current directory.")
        OUTPUT_DIR = './'
    file_path = os.path.join(OUTPUT_DIR, '%s_%s.csv')

    raw_url = os.environ.get('DATABASE_URL', '')
    DATABASE_URL = enforce_supabase_ssl(normalize_database_url(raw_url))

    try:
        parsed = urlparse(DATABASE_URL)
        logger.debug(f"DB host: {parsed.hostname}, port: {parsed.port}, db: {parsed.path.lstrip('/')}")
    except Exception:
        pass

    if not args.target_dates:
        logger.info('please set target date. e.g. 20200408. Comma-separate multiple dates.')

    target_dates = args.target_dates.split(',')
    for target_date in target_dates:
        is_dry = _truthy(args.dry_run)
        logger.info('running as dry run...' if is_dry else 'running as prd run...')
        insert_data(target_date, dry_run=is_dry, file_path_tpl=file_path, db_url=DATABASE_URL)

    # Note: Ensure the target date corresponds to an official release date for best results.
