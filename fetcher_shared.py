# pyright: reportMissingImports=false, reportMissingModuleSource=false

from typing import Dict, List, Optional, Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
import socket

import chardet
import requests

from const import CAO_WATCHER_BASE_URL, REQUEST_TIMEOUT_SEC, SOURCE_CONFIG


def parse_truthy(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in ('1', 'true', 'yes', 'y')


def decode_csv_bytes(content: bytes, logger) -> str:
    try:
        encoding = chardet.detect(content).get('encoding') or 'utf-8'
        logger.info(encoding)  # e.g., utf-8-sig, CP932
        if encoding.lower() in ('utf-8-sig', 'utf_8_sig'):
            return content.decode('utf-8-sig')
        if encoding.upper() in ('CP932', 'SHIFT_JIS', 'SHIFT-JIS', 'SJIS'):
            try:
                return content.decode('cp932')
            except UnicodeDecodeError:
                return content.decode('shift_jisx0213', 'ignore')
        return content.decode(encoding, errors='ignore')
    except Exception:
        try:
            return content.decode('utf-8')
        except Exception:
            return content.decode('shift_jisx0213', 'ignore')


def http_get(url: str, logger, timeout_sec: int = REQUEST_TIMEOUT_SEC) -> Optional[requests.Response]:
    try:
        return requests.get(url, timeout=timeout_sec)
    except requests.RequestException as exc:
        logger.error(f"request failed for {url}: {exc}")
        return None


def build_source_urls(today: str, source_config: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    parts = today.split('-')
    if len(parts) != 3:
        raise ValueError(f"today must be in YYYY-MM-DD format, got: {today}")

    year = parts[0]
    monthday = ''.join(parts[1:])
    urls: List[Dict[str, str]] = []
    for e in source_config:
        item = {
            'pattern': str(e['pattern']),
            'header_skip': int(e['header_skip']),
            'region': str(e['region']),
            'url': f"{CAO_WATCHER_BASE_URL}/{year}/{monthday}watcher/watcher{e['watcher']}.csv",
        }
        urls.append(item)
    return urls


def normalize_database_url(raw_url: str) -> str:
    if raw_url.startswith('postgres://'):
        return 'postgresql+psycopg2://' + raw_url[len('postgres://'):]
    if raw_url.startswith('postgresql://'):
        return 'postgresql+psycopg2://' + raw_url[len('postgresql://'):]
    if raw_url.startswith('postgresql+'):
        return raw_url
    return raw_url.replace('postgres://', 'postgresql+psycopg2://', 1)


def enforce_supabase_ssl(db_url: str) -> str:
    try:
        parsed = urlparse(db_url)
        if not parsed.hostname or 'supabase.co' not in parsed.hostname:
            return db_url
        query = dict(parse_qsl(parsed.query))
        if query.get('sslmode') is None:
            query['sslmode'] = 'require'
        new_query = urlencode(query)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        return db_url


def can_resolve_host(db_url: str, logger) -> bool:
    try:
        host = urlparse(db_url).hostname
        if not host:
            return False
        socket.gethostbyname(host)
        return True
    except Exception as exc:
        logger.error(f"DNS resolution failed for DB host: {exc}")
        return False
