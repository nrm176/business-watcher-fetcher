## BusinessWatcherBot

A small utility that downloads the Japanese Economy Watchers Survey CSVs, cleans and normalizes them, validates the records, saves CSV snapshots, and optionally inserts the validated rows into a Postgres database.

### Features
- Download monthly Economy Watchers Survey CSVs (national and Koshinetsu variants) from the Cabinet Office.
- Robust CSV decoding (UTF-8/CP932/Shift-JIS) with safe fallbacks.
- Normalize columns and values, including industry, job title, and region extraction.
- Score conversion for current/outlook fields; deterministic `id` generation via MD5.
- Validation using Pydantic models to ensure schema and value correctness.
- Outputs CSVs to a configurable directory:
  - `append_YYYY-MM-DD.csv` — full combined dataset for the run.
  - `invalid_YYYY-MM-DD.csv` — rows that failed validation (for auditing).
- Bulk insert of validated rows into Postgres using SQLAlchemy.

### Requirements
- Python 3.10 (runtime.txt uses 3.10.5)
- Dependencies in `requirements.txt`
- Internet access to fetch the monthly CSV files
- A Postgres database (only if you want to insert results)

### Environment variables
Set these via a local `.env` file (automatically loaded if present) or your OS environment.

- `DATABASE_URL` (required for DB insert): Postgres connection URL.
  - Examples:
    - `postgresql://user:pass@host:5432/dbname`
    - `postgres://...` (will be normalized to a SQLAlchemy driver URL)
    - For Supabase, `?sslmode=require` is enforced automatically.
- `BUSINESS_WATCHER_BOT_TABLE_NAME` (required for DB insert): target table name.
- `OUTPUT_DIR` (optional): directory where CSV outputs are written. Defaults to current directory (`./`).

### How to run (local)
The main entry point is `fetcher.py`. It expects one positional argument: a comma-separated list of target dates in `YYYYMMDD` format.

Basic run (inserts into DB if env vars are set):

```
python fetcher.py 20250111
```

Multiple dates (comma-separated):

```
python fetcher.py 20250111,20250208
```

Dry run (skip DB insert but still process and write CSV outputs):

```
python fetcher.py 20250111 --dry_run=1
```

Outputs are written to `OUTPUT_DIR` (or current directory by default):
- `append_YYYY-MM-DD.csv`
- `invalid_YYYY-MM-DD.csv` (only if there are invalid rows)

### Optional: Download historical raw CSVs
`download_csv.py` can download older survey CSV files into `./historical_data/`.

Example (uses the hardcoded target list in the script):

```
python download_csv.py
```

### Notes and troubleshooting
- If you see decoding issues, the script already applies chardet-based detection and fallbacks (UTF-8, CP932/Shift-JIS).
- If DB insert fails with SSL or driver errors, ensure:
  - `DATABASE_URL` is a valid Postgres URL; the script normalizes common forms to SQLAlchemy’s `postgresql+psycopg2://...`.
  - For Supabase, `sslmode=require` is appended automatically.
- If validation rejects rows, check `invalid_YYYY-MM-DD.csv` to review errors and the affected rows.

### Development
Install dependencies and run:

```
pip install -r requirements.txt
python fetcher.py 20250111 --dry_run=1
```

### Platform notes
- The tool is platform-agnostic. Provide configuration via `.env` or environment variables.
- You can control output location using `OUTPUT_DIR`.
