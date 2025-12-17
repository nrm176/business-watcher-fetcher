# create a test class for fetcher.py
import unittest
import fetcher
import os
class TestFetcher(unittest.TestCase):
    def test_fetch(self):

        fetcher.download_csv('https://www5.cao.go.jp/keizai3/2024/0112watcher/watcher5.csv')


        # check if the file is downloaded and exists
        self.assertTrue(os.path.exists('2024.0208watcher.5.csv'))


if __name__ == '__main__':
    unittest.main()