# pyright: reportMissingImports=false, reportMissingModuleSource=false
"""Pipeline stages for BusinessWatcherBot.

Each stage is a plain function that accepts an explicit logger so it can be
called and tested independently.

  Stage 1 – Fetch    : fetch_sources(today, source_config, logger)
  Stage 2 – Transform: assemble_dataframes(fetched, today_dt, logger)
  Stage 3 – Validate : validate_dataframe (from validation.py, used directly)
  Stage 4 – Persist  : persist_csv(...)  /  persist_db(...)
"""

import hashlib
import io
import json
import os
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

from const import (
    CONVERTER_MAP,
    KOSHINETSU_CONVERTER_CURRENT_MAP,
    KOSHINETSU_CONVERTER_OUTLOOK_MAP,
    RENAME_COLUMNS,
)
from fetcher_shared import build_source_urls, decode_csv_bytes, http_get


# ---------------------------------------------------------------------------
# Stage 1 – Fetch
# ---------------------------------------------------------------------------

def fetch_sources(
    today: str,
    source_config: List[Dict[str, Any]],
    logger,
) -> List[Dict[str, Any]]:
    """Download each source CSV.  Unavailable URLs are skipped (logged as errors).

    Returns a list of dicts with keys:
      raw         – decoded CSV text
      pattern     – 'current' | 'outlook'
      region      – 'all' | 'koshinetsu'
      header_skip – int, forwarded to pd.read_csv
    """
    results: List[Dict[str, Any]] = []
    for entry in build_source_urls(today, source_config):
        res = http_get(entry['url'], logger)
        if res is None or res.status_code != 200:
            logger.error(f"{entry['url']} not available")
            continue
        logger.info(f"doing url: {entry['url']}")
        results.append({
            'raw': decode_csv_bytes(res.content, logger),
            'pattern': entry['pattern'],
            'region': entry['region'],
            'header_skip': entry['header_skip'],
        })
    return results


# ---------------------------------------------------------------------------
# Stage 2 – Transform (private helpers + public assembler)
# ---------------------------------------------------------------------------

def _normalize_category(df: pd.DataFrame, pattern: str, region: str) -> pd.DataFrame:
    """Clean 分野, mask header-row values, derive 地域, forward-fill."""
    df['分野'] = df['分野'].apply(
        lambda x: x.replace('\n', '').replace('\u3000', '') if isinstance(x, str) else x
    )
    df['分野'] = df['分野'].apply(
        lambda x: np.nan if isinstance(x, str) and (x[0].isdigit() or x == '分野') else x
    )

    header_words = (
        ['景気の現状判断', '判断の理由', '追加説明及び具体的状況の説明', '業種・職種']
        if pattern == 'current'
        else ['景気の先行き判断', '景気の先行きに対する判断理由', '業種・職種']
    )
    for word in header_words:
        df[word] = df[word].apply(lambda x: np.nan if isinstance(x, str) and x == word else x)

    df['分野'] = df['分野'].ffill()

    if region == 'all':
        df['地域'] = df['分野'].apply(
            lambda x: x.split('(')[1].replace(')', '') if isinstance(x, str) else x
        )
    else:
        df['地域'] = '甲信越'

    df['分野'] = df['分野'].apply(
        lambda x: x.split('(')[0] if isinstance(x, str) and '(' in x else x
    )
    return df


def _parse_industry(df: pd.DataFrame) -> pd.DataFrame:
    """Split 業種・職種 into 業種, 業種詳細, and 職種 columns."""
    def _detail(x: str) -> Any:
        # Matches original splitter: returns np.nan for strings without '［'
        parts = x.split('［')
        return parts[1].replace('］', '') if len(parts) > 1 else np.nan

    df['業種'] = df['業種・職種'].apply(
        lambda x: x.split('（')[0] if isinstance(x, str) and '（' in x else x
    )
    df['職種'] = df['業種・職種'].apply(
        lambda x: x.split('（')[1].replace('）', '') if isinstance(x, str) and '（' in x else x
    )
    # _detail is called for all strings (including those without '［') to
    # preserve original behaviour where strings lacking '［' → np.nan.
    df['業種詳細'] = df['業種'].apply(lambda x: _detail(x) if isinstance(x, str) else x)
    df['業種'] = df['業種'].apply(
        lambda x: x.split('［')[0] if isinstance(x, str) and '［' in x else x
    )
    return df


def _apply_scores(df: pd.DataFrame, pattern: str, region: str) -> pd.DataFrame:
    """Map score symbols / Japanese words to floats."""
    if region == 'koshinetsu':
        if pattern == 'current':
            df['景気の現状判断'] = df['景気の現状判断'].ffill()
            df['景気の現状判断'] = df['景気の現状判断'].map(KOSHINETSU_CONVERTER_CURRENT_MAP)
        else:
            df['景気の先行き判断'] = df['景気の先行き判断'].ffill()
            df['景気の先行き判断'] = df['景気の先行き判断'].map(KOSHINETSU_CONVERTER_OUTLOOK_MAP)
    else:
        if pattern == 'current':
            df['景気の現状判断'] = df['景気の現状判断'].map(CONVERTER_MAP)
        else:
            df['景気の先行き判断'] = df['景気の先行き判断'].map(CONVERTER_MAP)
    return df


def _generate_id(df: pd.DataFrame) -> List[str]:
    """Deterministic MD5 hex digest over the string representation of each row."""
    hashes = []
    for _, row in df.iterrows():
        raw = ''.join(str(v) for v in row.values)
        hashes.append(hashlib.md5(raw.encode()).hexdigest())
    return hashes


def _transform_one(
    raw: str,
    date: datetime,
    header_skip: int,
    pattern: str,
    region: str,
) -> pd.DataFrame:
    """Parse and normalize one raw CSV string into a renamed DataFrame."""
    df = pd.read_csv(io.StringIO(raw), header=header_skip)
    df = df.replace('\n', '', regex=True)
    df.rename(columns={'Unnamed: 1': '都道府県'}, inplace=True)

    # Pre-initialize derived columns so downstream ops always find them
    df.loc[:, '地域'] = np.nan
    df.loc[:, '業種'] = np.nan
    df.loc[:, '業種詳細'] = np.nan
    df.loc[:, '職種'] = np.nan

    df = _normalize_category(df, pattern, region)
    df = _parse_industry(df)

    if pattern == 'current':
        df['追加説明及び具体的状況の説明'] = df['追加説明及び具体的状況の説明'].apply(
            lambda x: x.replace('・', '') if isinstance(x, str) else x
        )
        df = df[df['判断の理由'] != '＊']
        df = df[df['判断の理由'] != '−']
    else:
        df['景気の先行きに対する判断理由'] = df['景気の先行きに対する判断理由'].apply(
            lambda x: x.replace('・', '') if isinstance(x, str) else x
        )
        df = df[df['景気の先行きに対する判断理由'] != '＊']
        df = df[df['景気の先行きに対する判断理由'] != '−']

    drop_thresh = 5 if region == 'koshinetsu' else 4
    df = df.dropna(thresh=drop_thresh)
    df = df.drop(columns='業種・職種')

    df = _apply_scores(df, pattern, region)
    df['タイプ'] = '現状' if pattern == 'current' else '先行き'
    df['日付'] = date
    df = df.rename(columns=RENAME_COLUMNS)
    return df


def _clean_one(df: pd.DataFrame, pattern: str, region: str) -> pd.DataFrame:
    """Derive score / comments / reason, generate id hash, deduplicate."""
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')].copy()

    # reason is blank for koshinetsu (no free-text reason column) and for outlook
    if region == 'koshinetsu':
        df['reason'] = ''
    if pattern == 'outlook':
        df['reason'] = ''
        # comments comes from reason_future for outlook rows
        df['comments'] = df['reason_future'].apply(lambda x: '' if pd.isnull(x) else x)
        df['score'] = df['score_future'].apply(lambda x: 1.0 if pd.isnull(x) else x)
        df = df.drop(columns=['reason_future', 'score_future'])
    else:
        df['score'] = df['score_current'].apply(lambda x: 1.0 if pd.isnull(x) else x)
        df = df.drop(columns=['score_current'])

    df['id'] = _generate_id(
        df[['dtype', 'category', 'reason', 'region', 'dt', 'comments', 'industry', 'score']]
    )
    df = df.drop_duplicates(subset=['id'])
    df = df.set_index('id', verify_integrity=True)
    return df


def assemble_dataframes(
    fetched: List[Dict[str, Any]],
    today_dt: datetime,
    logger,
) -> Optional[pd.DataFrame]:
    """Transform each fetched entry and concatenate into one DataFrame.

    Entries that fail to transform are logged and skipped.
    Returns None when no entries could be transformed.
    """
    dfs: List[pd.DataFrame] = []
    for item in fetched:
        try:
            raw_df = _transform_one(
                item['raw'], today_dt, item['header_skip'], item['pattern'], item['region']
            )
            clean_df = _clean_one(raw_df, item['pattern'], item['region'])
            dfs.append(clean_df)
        except Exception as exc:
            logger.error(
                f"Transform failed for pattern={item['pattern']} region={item['region']}: {exc}"
            )
    return pd.concat(dfs, sort=False) if dfs else None


# ---------------------------------------------------------------------------
# Stage 4a – Persist CSV outputs
# ---------------------------------------------------------------------------

def persist_csv(
    append_df: pd.DataFrame,
    valid_df: Optional[pd.DataFrame],
    invalid_df: Optional[pd.DataFrame],
    errors: List[Dict],
    file_path_tpl: str,
    today: str,
    logger,
) -> None:
    """Write append, invalid-rows, and error-log files to *file_path_tpl*."""
    try:
        path = file_path_tpl % ('append', today)
        append_df.to_csv(path, encoding='utf-8')
        logger.info(f'saving at {path}')
        logger.debug(f'saving at {path}')

        if invalid_df is not None and not invalid_df.empty:
            invalid_path = file_path_tpl % ('invalid', today)
            invalid_df.to_csv(invalid_path, encoding='utf-8')
            logger.info(f'invalid rows saved at {invalid_path}')

        if errors:
            errors_csv = file_path_tpl % ('errors', today)
            errors_json = os.path.splitext(errors_csv)[0] + '.json'
            pd.DataFrame(errors).to_csv(errors_csv, index=False, encoding='utf-8')

            def _json_serial(obj):
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                try:
                    if isinstance(obj, float) and np.isnan(obj):
                        return None
                except Exception:
                    pass
                return str(obj)

            with open(errors_json, 'w', encoding='utf-8') as jf:
                json.dump(errors, jf, ensure_ascii=False, indent=2, default=_json_serial)
            top = Counter(e.get('errors', '') for e in errors).most_common(5)
            logger.info(f'Saved error logs: {errors_csv} and {errors_json}')
            logger.info('Top validation errors: ' + '; '.join(f'{m} x{c}' for m, c in top))
    except Exception as exc:
        logger.error(f'error writing CSV outputs: {exc}')


# ---------------------------------------------------------------------------
# Stage 4b – Persist DB (upsert)
# ---------------------------------------------------------------------------

def persist_db(
    engine,
    df: pd.DataFrame,
    table_name: str,
    mode: str = 'nothing',
) -> int:
    """Upsert *df* into Postgres via ON CONFLICT on primary key 'id'.

    mode='nothing' → DO NOTHING (safe deduplication, default)
    mode='update'  → DO UPDATE (overwrite all non-PK columns)

    Returns the total number of rows attempted (not necessarily affected).
    """
    if df is None or df.empty:
        return 0

    records = df.reset_index().to_dict(orient='records')
    metadata = MetaData()
    total = 0
    chunk_size = 1000
    with engine.begin() as conn:
        tbl = Table(table_name, metadata, autoload_with=conn)
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            stmt = pg_insert(tbl).values(chunk)
            if mode == 'update':
                update_cols = {
                    c.name: getattr(stmt.excluded, c.name)
                    for c in tbl.columns if c.name != 'id'
                }
                stmt = stmt.on_conflict_do_update(index_elements=['id'], set_=update_cols)
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=['id'])
            conn.execute(stmt)
            total += len(chunk)
    return total
