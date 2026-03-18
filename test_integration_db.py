# pyright: reportMissingImports=false

import os
import unittest
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

from sqlalchemy import create_engine, text


def _normalize_db_url(raw_url: str) -> str:
    if raw_url.startswith('postgres://'):
        return 'postgresql+psycopg2://' + raw_url[len('postgres://'):]
    if raw_url.startswith('postgresql://'):
        return 'postgresql+psycopg2://' + raw_url[len('postgresql://'):]
    return raw_url


def _enforce_ssl_if_supabase(db_url: str) -> str:
    parsed = urlparse(db_url)
    if not parsed.hostname or 'supabase.co' not in parsed.hostname:
        return db_url

    query = dict(parse_qsl(parsed.query))
    if 'sslmode' not in query:
        query['sslmode'] = 'require'
    return urlunparse((
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        urlencode(query),
        parsed.fragment,
    ))


@unittest.skipUnless(os.getenv('DATABASE_URL'), 'DATABASE_URL is required for integration tests')
class TestDatabaseIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        raw = os.environ.get('DATABASE_URL', '')
        normalized = _normalize_db_url(raw)
        cls.db_url = _enforce_ssl_if_supabase(normalized)
        cls.engine = create_engine(cls.db_url)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.engine.dispose()

    def test_db_connectivity(self):
        with self.engine.connect() as conn:
            result = conn.execute(text('SELECT 1')).scalar_one()
        self.assertEqual(result, 1)

    @unittest.skipUnless(
        os.getenv('BUSINESS_WATCHER_BOT_TABLE_NAME'),
        'BUSINESS_WATCHER_BOT_TABLE_NAME is required for table metadata integration test',
    )
    def test_target_table_is_accessible(self):
        table_name = os.environ.get('BUSINESS_WATCHER_BOT_TABLE_NAME')
        query = text(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = current_schema()
                AND table_name = :table_name
            )
            """
        )
        with self.engine.connect() as conn:
            exists = conn.execute(query, {'table_name': table_name}).scalar_one()
        self.assertTrue(bool(exists))


if __name__ == '__main__':
    unittest.main()
