# BusinessWatcherBot – Copilot Instructions

## Project overview

Downloads the Japanese Cabinet Office **Economy Watchers Survey** CSV files, normalizes and cleans the data, validates records with Pydantic, writes CSV snapshots, and optionally upserts validated rows into a Postgres database (Supabase-compatible).

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the pipeline (dry run – no DB insert)
python fetcher.py 20250111 --dry_run=1

# Run the pipeline (with DB insert)
python fetcher.py 20250111

# Multiple dates (comma-separated)
python fetcher.py 20250111,20250208

# Run all tests
python -m unittest -v

# Run a single test
python -m unittest -v test_debug.TestFetcher.test_create_file_name

# Integration tests (require a live DB)
DATABASE_URL='postgresql://user:pass@host:5432/dbname' \
BUSINESS_WATCHER_BOT_TABLE_NAME='your_table' \
python -m unittest -v test_integration_db.py
```

## Architecture

The pipeline is split across four files with clear stage boundaries:

| File | Role |
|---|---|
| `fetcher.py` | CLI entry point and thin orchestrator; calls pipeline stages |
| `pipeline.py` | All four pipeline stages as plain, independently testable functions |
| `fetcher_shared.py` | Pure utilities: HTTP, CSV decoding, URL building, DB URL normalization |
| `const.py` | All constants: source URLs, score maps, column rename map, `SOURCE_CONFIG` |
| `validation.py` | Pydantic `WatcherRecord` model + `validate_dataframe()` |

**Data flow:**

```
fetch_sources()       → List of raw CSV strings
assemble_dataframes() → Single normalized pandas DataFrame (id as index)
validate_dataframe()  → (valid_df, invalid_df, errors list)
persist_csv()         → append/invalid/errors CSV+JSON files
persist_db()          → Postgres upsert via SQLAlchemy (ON CONFLICT DO NOTHING/UPDATE)
```

Four source CSV variants are fetched per run (defined in `SOURCE_CONFIG` in `const.py`):
- national current (`watcher4.csv`) and outlook (`watcher5.csv`)
- Koshinetsu regional current (`watcher6.csv`) and outlook (`watcher7.csv`)

## Key conventions

**Score encoding:** Survey symbols (`◎ ○ □ ▲ ×`) map to floats `1.0 → 0.0` via `CONVERTER_MAP` in `const.py`. Koshinetsu uses text words instead; those have separate maps (`KOSHINETSU_CONVERTER_CURRENT_MAP`, `KOSHINETSU_CONVERTER_OUTLOOK_MAP`).

**Deterministic row IDs:** Each row's `id` is an MD5 hex digest of `dtype + category + reason + region + dt + comments + industry + score`. Used as the Postgres primary key for idempotent upserts.

**Column naming:** Japanese column names are used throughout transformation and renamed to English at the end of `_transform_one()` via `RENAME_COLUMNS` in `const.py`. Never use Japanese column names after the rename step.

**Two pipeline patterns:** `pattern='current'` uses `score_current`/`reason`/`comments`; `pattern='outlook'` uses `score_future`/`reason_future` and collapses them to `score`/`comments` in `_clean_one()`.

**DB URL normalization:** `postgres://` and `postgresql://` are always rewritten to `postgresql+psycopg2://`. Supabase URLs automatically get `sslmode=require` injected. These helpers live in `fetcher_shared.py`.

**Environment variables:** Loaded from `.env` at startup (via `python-dotenv`). Required for DB insert:
- `DATABASE_URL` – Postgres connection string
- `BUSINESS_WATCHER_BOT_TABLE_NAME` – target table name
- `OUTPUT_DIR` (optional) – output directory, defaults to `./`
- `UPSERT_MODE` (optional) – `nothing` (default) or `update`

**CSV encoding:** Source files may be UTF-8-sig, CP932, or Shift-JIS. `decode_csv_bytes()` in `fetcher_shared.py` uses `chardet` for detection with safe fallbacks.

**Test isolation:** Unit tests in `test_debug.py` use no mocks and no network; they test pure functions with in-memory data. Integration tests in `test_integration_db.py` are skipped automatically when `DATABASE_URL` is unset.

**`fetcher.py` public API:** Thin wrapper functions (`_get`, `_truthy`, `_decode_csv_bytes`, etc.) exist purely for backwards compatibility with external callers and tests — the real logic lives in `fetcher_shared.py` and `pipeline.py`.
