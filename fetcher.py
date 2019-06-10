import pandas as pd
import re
from datetime import datetime, timedelta
import numpy as np
import requests
import io
import csv
from os.path import join, dirname
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from logging import getLogger, StreamHandler, DEBUG
import hashlib
from pathlib import Path
import psycopg2

logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False

keys = ['◎', '○', '□', '▲', '×']
values = [1.0, 0.75, 0.5, 0.25, 0.0]
CONVERTER_MAP = {}
KOSHINETSU_CONVERTER_OUTLOOK_MAP = {'良くなる': 1.0, 'やや良くなる': 0.75, '変わらない': 0.5, 'やや悪くなる': 0.25, '悪くなる': 0.0}
KOSHINETSU_CONVERTER_CURRENT_MAP = {'良くなっている': 1.0, 'やや良くなっている': 0.75, '変わらない': 0.5, 'やや悪くなっている': 0.25, '悪くなっている': 0.0}
RENAME_COLUMNS = {'Id': 'id', 'タイプ': 'dtype', '分野': 'category',
                  '判断の理由': 'reason', '地域': 'region', '日付': 'dt',
                  '景気の先行きに対する判断理由': 'reason_future', '景気の先行き判断': 'score_future', '景気の現状判断': 'score_current',
                  '業種': 'industry', '業種詳細': 'industry_detail', '職種': 'job_title', '追加説明及び具体的状況の説明': 'comments',
                  '都道府県': 'pref'}

for k, v in zip(keys, values):
    CONVERTER_MAP[k] = v


def to_csv(data, file=None):
    if file:
        with open(file, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            reader = csv.reader(data.splitlines())
            for row in reader:
                writer.writerow(row)

BASE_PATH = './historical_data/%s.%s.%s.csv'

def create_dataframe(data, date, header_skip, pattern, region):
    df = pd.read_csv(io.StringIO(data), header=header_skip, encoding="utf-8")

    df = df.replace('\n', '', regex=True)
    df.rename(columns={'Unnamed: 1': '都道府県'}, inplace=True)
    # remove new line and \u3000
    df['分野'] = list(map(lambda x: x.replace('\n', '').replace('\u3000', '') if type(x) == str else x, df['分野']))

    # fill nan if x[0] is digit or x is a unwanted word
    df['分野'] = list(map(lambda x: np.nan if type(x) == str and
                                            (x[0].isdigit() or x == '分野') else x, df['分野']))

    # 下記のwordが入っている行はnanとする

    if pattern == 'current':
        words = ['景気の現状判断', '判断の理由', '追加説明及び具体的状況の説明', '業種・職種']
    else:
        words = ['景気の先行き判断', '景気の先行きに対する判断理由', '業種・職種']

    for word in words:
        df[word] = list(map(lambda x: np.nan if type(x) == str and
                                                x == word else x, df[word]))

    # fill nan with previous value
    df['分野'] = df['分野'].fillna(method='ffill')

    # add new columns
    df.loc[:, '地域'] = np.nan
    df.loc[:, '業種'] = np.nan
    df.loc[:, '業種詳細'] = np.nan
    df.loc[:, '職種'] = np.nan
    # もともとの分野を括弧を元に分割する。分野と地域に別れる。
    if region == 'all':
        df['地域'] = list(map(lambda x: x.split('(')[1].replace(')', '') if type(x) == str else x, df['分野']))
    else:
        df['地域'] = '甲信越'

    df['分野'] = list(map(lambda x: x.split('(')[0] if type(x) == str and '(' in x else x, df['分野']))

    df['業種'] = list(map(lambda x: x.split('（')[0] if type(x) == str and '（' in x else x, df['業種・職種']))
    df['職種'] = list(map(lambda x: x.split('（')[1].replace('）', '') if type(x) == str and '（' in x else x, df['業種・職種']))

    # 説明欄にある特殊文字は不要
    def spliter(x, delimiter=None, remover=None):
        if delimiter and remover:
            if len(x.split(delimiter)) > 1:
                return x.split(delimiter)[1].replace(remover, '')
            return np.nan

    df['業種詳細'] = list(map(lambda x: spliter(x, '［', '］') if type(x) == str else x, df['業種']))
    df['業種'] = list(map(lambda x: x.split('［')[0] if type(x) == str and '［' in x else x, df['業種']))

    if pattern == 'current':
        df['追加説明及び具体的状況の説明'] = list(map(lambda x: x.replace('・', '') if type(x) == str else x, df['追加説明及び具体的状況の説明']))
        df = df[df['判断の理由'] != '＊']
        df = df[df['判断の理由'] != '−']
    else:
        df['景気の先行きに対する判断理由'] = list(map(lambda x: x.replace('・', '') if type(x) == str else x, df['景気の先行きに対する判断理由']))
        df = df[df['景気の先行きに対する判断理由'] != '＊']
        df = df[df['景気の先行きに対する判断理由'] != '−']

    # drop a row by condition
    if region == 'all':
        df = df.dropna(thresh=4)

    if region == 'koshinetsu':
        df = df.dropna(thresh=5)

    # 業種・職種は別々のカラムを作ったので不要
    df = df.drop('業種・職種', 1)

    # 甲信越はスコア表記が他の地域と違う
    if region == 'koshinetsu':
        if pattern == 'current':
            df['景気の現状判断'] = df['景気の現状判断'].fillna(method='ffill')
            df['景気の現状判断'] = list(map(lambda x: KOSHINETSU_CONVERTER_CURRENT_MAP.get(x), df['景気の現状判断']))
        else:
            df['景気の先行き判断'] = df['景気の先行き判断'].fillna(method='ffill')
            df['景気の先行き判断'] = list(map(lambda x: KOSHINETSU_CONVERTER_OUTLOOK_MAP.get(x), df['景気の先行き判断']))
    else:
        if pattern == 'current':
            df['景気の現状判断'] = list(map(lambda x: CONVERTER_MAP.get(x), df['景気の現状判断']))
        else:
            df['景気の先行き判断'] = list(map(lambda x: CONVERTER_MAP.get(x), df['景気の先行き判断']))

    if pattern == 'current':
        df['タイプ'] = '現状'
    else:
        df['タイプ'] = '先行き'

    df['日付'] = date

    df = df.rename(columns=RENAME_COLUMNS)

    return df


def generateHash(df):
    hashes = []
    for idx, row in df.iterrows():
        xs = list(zip(row, row.index))
        t = tuple(tuple(x) for x in xs)

        # h = hashlib.new(t)
        r = ''.join(list(map(lambda t: str(t[0]), t)))
        h = hashlib.md5(str.encode(r))

        hashes.append(h.hexdigest())
    return hashes


def patternCounter(series):
    d = {}
    for e in series:
        if d.get(e) is None:
            d[e] = 1
        else:
            d[e] = d[e] + 1
    return d


def retrieve_csv_file(url):
    res = requests.get(url)
    if res.status_code != 200:
        return None
    data = res.content.decode('shift_jisx0213')
    return data


def insert_everything():
    pass


def construct_urls(today):
    return [
        {'pattern': 'outlook',
         'url': 'http://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher5.csv' % tuple(today.split('-')),
         'header_skip': 7, 'region': 'all'},
        {'pattern': 'current',
         'url': 'http://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher4.csv' % tuple(today.split('-')),
         'header_skip': 7, 'region': 'all'},
        {'pattern': 'outlook',
         'url': 'http://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher7.csv' % tuple(today.split('-')),
         'header_skip': 2, 'region': 'koshinetsu'},
        {'pattern': 'current',
         'url': 'http://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher6.csv' % tuple(today.split('-')),
         'header_skip': 2, 'region': 'koshinetsu'}
    ]

def construct_path():
    return [
        {'pattern': 'outlook', 'header_skip': 7, 'region': 'all'},
        {'pattern': 'current', 'header_skip': 7, 'region': 'all'},
        {'pattern': 'outlook', 'header_skip': 2, 'region': 'koshinetsu'},
        {'pattern': 'current', 'header_skip': 2, 'region': 'koshinetsu'}
    ]


def clean_data_frame(df, pattern, region):
    # Remove unamed column
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

    if region == 'koshinetsu':
        df['reason'] = ''

    if pattern == 'outlook':
        df['reason'] = ''

    if pattern == 'outlook':
        df["comments"] = ''
        df["comments"] = df["reason_future"].apply(lambda x: '' if pd.isnull(x) else x) + df["comments"].apply(
            lambda x: '' if pd.isnull(x) else x)

    if pattern == 'outlook':
        df['score'] = df['score_future'].apply(lambda x: 1.0 if pd.isnull(x) else x) * 1
    else:
        df['score'] = df['score_current'].apply(lambda x: 1.0 if pd.isnull(x) else x)

    # Has just combined reason_future and comments, so drop reason_future
    if pattern == 'outlook':
        df = df.drop(columns=['reason_future', 'score_future', ])
    else:
        df = df.drop(columns=['score_current'])

    df['id'] = generateHash(
        df[['dtype', 'category', 'reason', 'region', 'dt', 'comments',
            'industry', 'score']])

    df = df.set_index('id')

    return df


def construct_data_frame_v2(e, today_dt):
    d = retrieve_csv_file(e['url'])
    if d:
        logger.info('doing url: %s' % e['url'])
        df = create_dataframe(d, date=today_dt, header_skip=e['header_skip'], pattern=e['pattern'], region=e['region'])
        if df is not None:
            df = clean_data_frame(df, pattern=e['pattern'], region=e['region'])
            return df
    else:
        logger.error('%s not available' % e['url'])
    return None

def construct_data_frame_v3(path, yyyymmdddate, today_dt):

    logger.info('doing url: %s' % path)
    df = create_dataframe(path, yyyymmdddate=yyyymmdddate, date=today_dt, header_skip=path['header_skip'], pattern=path['pattern'], region=path['region'])
    if df is not None:
        df = clean_data_frame(df, pattern=path['pattern'], region=path['region'])
        return df


def to_yyyy_mm_dd(dt_str):
    today_dt = datetime.strptime(dt_str, '%Y%m%d')
    today = datetime.strftime(today_dt, '%Y-%m-%d')
    return today


def insert_data(target_date, MANUAL_RUN=True):
    if MANUAL_RUN:
        dt_str = target_date
        today_dt = datetime.strptime(dt_str, '%Y%m%d')
        today = datetime.strftime(today_dt, '%Y-%m-%d')
    else:
        today_dt = datetime.strptime(datetime.today(), '%Y%m%d')
        today = datetime.strftime(today_dt, '%Y-%m-%d')

    urls = construct_urls(today)

    dfs = []
    for url in urls:
        dfs.append(construct_data_frame_v2(url, today_dt))

    if len(dfs) >= 1:
        append_df = pd.concat(dfs)
        engine = create_engine(DATABASE_URL)

        if ON_HEROKU:
            # engine = create_engine(DATABASE_URL)
            try:
                append_df.to_sql(os.environ['BUSINESS_WATCHER_BOT_TABLE_NAME'], engine, if_exists='append')
                logger.debug('records as of {0} have been saved to Heroku postgres'.format(today))
            except Exception  as e:
                logger.error('error on insert: {0}'.format(e))
        else:
            append_df.to_csv(file_path % ('append', today), encoding='utf-8')
            append_df.to_sql(os.environ['BUSINESS_WATCHER_BOT_TABLE_NAME'], engine, if_exists='append')
            logger.debug('saving at %s' % file_path % ('append', today))

    # TODO:
    #   Step1. Run the script on heroku by doing ```heroku run python fetcher.py```
    #   Step2. Make sure dt_str is when the data is released

if __name__ == '__main__':

    ON_HEROKU = os.environ.get("ON_HEROKU", False)

    file_path = '/tmp/%s_%s.csv' if ON_HEROKU else './%s_%s.csv'

    if not ON_HEROKU:
        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)

    DATABASE_URL = os.environ["DATABASE_URL"]

    target_dates = ['20190610']
    for target_date in target_dates:
        insert_data(target_date)

    # TODO:
    #   Step1. Run the script on heroku by doing ```heroku run python fetcher.py```
    #   Step2. Make sure dt_str is when the data is released
