import unittest
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd

import fetcher
import pipeline


class TestFetcher(unittest.TestCase):
    def test_create_file_name(self):
        url = 'https://www5.cao.go.jp/keizai3/2024/0112watcher/watcher5.csv'
        self.assertEqual(fetcher.create_file_name(url), '2024.0112watcher.5.csv')

    def test_truthy_parser(self):
        self.assertTrue(fetcher._truthy('1'))
        self.assertTrue(fetcher._truthy('yes'))
        self.assertTrue(fetcher._truthy('TRUE'))
        self.assertFalse(fetcher._truthy('0'))
        self.assertFalse(fetcher._truthy('no'))
        self.assertFalse(fetcher._truthy(None))

    def test_construct_urls_shape(self):
        urls = fetcher.construct_urls('2026-03-09')
        self.assertEqual(len(urls), 4)
        self.assertTrue(all('url' in u for u in urls))
        self.assertTrue(all(u['url'].startswith('https://') for u in urls))

    def test_assemble_dataframes_returns_none_for_empty_input(self):
        result = pipeline.assemble_dataframes([], datetime(2026, 3, 9), fetcher.logger)
        self.assertIsNone(result)

    def test_persist_csv_writes_append_and_errors(self):
        append_df = pd.DataFrame([
            {
                'dtype': '現状',
                'category': '家計動向関連',
                'reason': 'sample',
                'region': '北海道',
                'dt': '2026-03-09',
                'comments': 'comment',
                'industry': '小売',
                'score': 0.5,
            }
        ])
        valid_df = append_df.copy()
        invalid_df = pd.DataFrame([
            {'id': 'bad-id', 'reason': 'invalid'}
        ])
        errors = [
            {'index': 'bad-id', 'errors': 'sample validation error', 'row': {'id': 'bad-id'}}
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            template = str(Path(tmpdir) / '%s_%s.csv')
            pipeline.persist_csv(
                append_df=append_df,
                valid_df=valid_df,
                invalid_df=invalid_df,
                errors=errors,
                file_path_tpl=template,
                today='2026-03-09',
                logger=fetcher.logger,
            )

            self.assertTrue((Path(tmpdir) / 'append_2026-03-09.csv').exists())
            self.assertTrue((Path(tmpdir) / 'invalid_2026-03-09.csv').exists())
            self.assertTrue((Path(tmpdir) / 'errors_2026-03-09.csv').exists())
            self.assertTrue((Path(tmpdir) / 'errors_2026-03-09.json').exists())


if __name__ == '__main__':
    unittest.main()