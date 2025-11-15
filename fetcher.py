import pandas as pd
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
import random
import string
import argparse
import chardet
from const import KOSHINETSU_CONVERTER_CURRENT_MAP, KOSHINETSU_CONVERTER_OUTLOOK_MAP, CONVERTER_MAP, RENAME_COLUMNS
import logging


logger = getLogger(__name__)
handler = StreamHandler()
handler.setLevel(DEBUG)
logger.setLevel(DEBUG)
logger.addHandler(handler)
logger.propagate = False


def to_csv(data, file=None):
    if file:
        with open(file, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            reader = csv.reader(data.splitlines())
            for row in reader:
                writer.writerow(row)


BASE_PATH = './historical_data/%s.%s.%s.csv'


def create_dataframe(data, date, header_skip, pattern, region):
    # check encoding using chardet

    df = pd.read_csv(io.StringIO(data), header=header_skip)

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
    # もともとの分野を括弧を元に分割する。分野と地域に分かれる。
    if region == 'all':
        df['地域'] = list(map(lambda x: x.split('(')[1].replace(')', '') if type(x) == str else x, df['分野']))
    else:
        df['地域'] = '甲信越'

    df['分野'] = list(map(lambda x: x.split('(')[0] if type(x) == str and '(' in x else x, df['分野']))

    df['業種'] = list(map(lambda x: x.split('（')[0] if type(x) == str and '（' in x else x, df['業種・職種']))
    df['職種'] = list(map(lambda x: x.split('（')[1].replace('）', '') if type(x) == str and '（' in x else x, df['業種・職種']))

    # 説明欄にある特殊文字は不要
    def splitter(x, delimiter=None, remover=None):
        if delimiter and remover:
            if len(x.split(delimiter)) > 1:
                return x.split(delimiter)[1].replace(remover, '')
            return np.nan

    df['業種詳細'] = list(map(lambda x: splitter(x, '［', '］') if type(x) == str else x, df['業種']))
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
    df = df.drop(columns='業種・職種')

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


def randomString(stringLength=10):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def generate_hash(df):
    hashes = []
    for idx, row in df.iterrows():
        xs = list(zip(row, row.index))
        t = tuple(tuple(x) for x in xs)

        # h = hashlib.new(t)
        r = ''.join(list(map(lambda t: str(t[0]), t)))
        # r += randomString(10)
        h = hashlib.md5(str.encode(r))

        hashes.append(h.hexdigest())
    return hashes


def pattern_counter(series):
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

    #check the encoding of res.content
    encoding = chardet.detect(res.content)['encoding']

    #

    try:
        data = res.content.decode(encoding)
    except UnicodeDecodeError:
        data = res.content.decode('shift_jisx0213', 'ignore')  # or 'replace'
    return data


def insert_everything():
    pass


def construct_urls(today):
    key = tuple(today.split('-'))
    return [
        {'pattern': 'outlook',
         'url': 'https://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher5.csv' % key,
         'header_skip': 7, 'region': 'all'},
        {'pattern': 'current',
         'url': 'https://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher4.csv' % key,
         'header_skip': 7, 'region': 'all'},
        {'pattern': 'outlook',
         'url': 'https://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher7.csv' % key,
         'header_skip': 2, 'region': 'koshinetsu'},
        {'pattern': 'current',
         'url': 'https://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher6.csv' % key,
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
    # Work on a copy to avoid chained assignment warnings
    df = df.copy()

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

    df['id'] = generate_hash(
        df[['dtype', 'category', 'reason', 'region', 'dt', 'comments',
            'industry', 'score']])

    df = df.drop_duplicates(subset=['id'])

    df = df.set_index('id', verify_integrity=True)

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
    df = create_dataframe(path, yyyymmdddate=yyyymmdddate, date=today_dt, header_skip=path['header_skip'],
                          pattern=path['pattern'], region=path['region'])
    if df is not None:
        df = clean_data_frame(df, pattern=path['pattern'], region=path['region'])
        return df


def to_yyyy_mm_dd(dt_str):
    today_dt = datetime.strptime(dt_str, '%Y%m%d')
    today = datetime.strftime(today_dt, '%Y-%m-%d')
    return today

def create_file_name(url):
    url_parts = url.split('/')

    # Extract the date and the number
    date = url_parts[-3:-1]
    number = url_parts[-1].replace('watcher', '').replace('.csv', '')

    # Join them with periods to form the filename
    file_name = '.'.join(date + [number])
    return f"{file_name}.csv"

def download_csv(url):
    res = requests.get(url)
    if res.status_code != 200:
        return None

    # first, check the encoding of res.content
    encoding = chardet.detect(res.content)['encoding']
    logging.info(encoding) #utf-8-sig, cp932
    try:
        if encoding == 'utf-8-sig':
            data = res.content.decode('utf-8-sig')
        elif encoding == 'CP932':
            data = res.content.decode('cp932')
        else:
            data = res.content.decode('shift_jisx0213')
    except UnicodeDecodeError:
        data = res.content.decode('shift_jisx0213', 'ignore')  # or 'replace'

    # save data to csv
    # create a file name from url and save it. the url format is as follows:
    # https://www5.cao.go.jp/keizai3/2024/0308watcher/watcher5.csv
    # in this case, the file name is 2024.0308watcher.watch5.csv
    file_name = create_file_name(url)

    with open(file_name, 'w', encoding='utf-8') as f:
        writer = csv.writer(f)
        reader = csv.reader(data.splitlines())
        for row in reader:
            writer.writerow(row)

def _enforce_supabase_ssl(db_url: str) -> str:
    try:
        from urllib.parse import urlparse, urlunparse, urlencode, parse_qsl
        parsed = urlparse(db_url)
        if not parsed.hostname or 'supabase.co' not in parsed.hostname:
            return db_url
        # merge existing query params and add sslmode=require if missing
        q = dict(parse_qsl(parsed.query))
        if q.get('sslmode') is None:
            q['sslmode'] = 'require'
        new_query = urlencode(q)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        return db_url


def _can_resolve_host(db_url: str) -> bool:
    try:
        from urllib.parse import urlparse
        import socket
        host = urlparse(db_url).hostname
        if not host:
            return False
        socket.gethostbyname(host)
        return True
    except Exception as e:
        logger.error(f"DNS resolution failed for DB host: {e}")
        return False


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
        append_df = pd.concat(dfs, sort=False)
        # Ensure Supabase uses SSL and DNS resolves before connecting
        safe_db_url = _enforce_supabase_ssl(DATABASE_URL)
        if not _can_resolve_host(safe_db_url):
            logger.error("Cannot resolve database host. Check your internet/DNS settings or the DATABASE_URL hostname.")
            return
        engine = create_engine(safe_db_url)
        try:
            logger.info('saving at %s' % file_path % ('append', today))
            append_df.to_csv(file_path % ('append', today), encoding='utf-8')
            logger.debug('saving at %s' % file_path % ('append', today))
        except Exception as e:
            logger.error('error on creating a csv file: {0}'.format(e))

        try:
            with engine.begin() as connection:
                append_df.to_sql(os.environ['BUSINESS_WATCHER_BOT_TABLE_NAME'], con=connection, if_exists='append')
                logger.debug('records as of {0} have been saved to postgres'.format(today))
        except Exception as e:
            logger.error('error on insert: {0}'.format(e))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Arguments of report downloader')
    parser.add_argument('target_dates', help='Set a target date to download data')
    parser.add_argument('--dry_run', help='set true if dry run')

    args = parser.parse_args()

    ON_HEROKU = os.environ.get("DYNO", False)
    file_path = '/tmp/%s_%s.csv' if ON_HEROKU else './%s_%s.csv'

    if not ON_HEROKU:
        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)

    # Normalize DATABASE_URL without corrupting username or driver
    raw_url = os.environ.get("DATABASE_URL", "")
    DATABASE_URL = raw_url
    if raw_url.startswith('postgres://'):
        DATABASE_URL = 'postgresql+psycopg2://' + raw_url[len('postgres://'):]
    elif raw_url.startswith('postgresql://'):
        DATABASE_URL = 'postgresql+psycopg2://' + raw_url[len('postgresql://'):]
    elif raw_url.startswith('postgresql+'):  # keep provided driver
        DATABASE_URL = raw_url
    else:
        # Fallback: if user accidentally provided just 'postgres', upgrade safely
        DATABASE_URL = raw_url.replace('postgres://', 'postgresql+psycopg2://', 1)

    # Enforce SSL for Supabase
    DATABASE_URL = _enforce_supabase_ssl(DATABASE_URL)

    # Lightweight log of target host for troubleshooting (no credentials)
    try:
        from urllib.parse import urlparse
        parsed = urlparse(DATABASE_URL)
        logger.debug(f"DB host: {parsed.hostname}, port: {parsed.port}, db: {parsed.path.lstrip('/')}")
    except Exception:
        pass

    if not args.target_dates:
        logger.info(
            'please set target date. i.e., --target_dates=20200408. --target_dates=20200101,20200202 if more than two')

    target_dates = args.target_dates.split(',')
    for target_date in target_dates:
        if args.dry_run == '1':
            logger.info('running as dry run...')
        else:
            logger.info('running as prd run...')
            insert_data(target_date)

    # TODO:
    #   Step1. Run the script on heroku by doing ```heroku run python fetcher.py yyyymmdd```
    #   Step2. Make sure dt_str is when the data is released
