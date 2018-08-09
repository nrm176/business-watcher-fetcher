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

RENAME_COLUMNS = {'Id' : 'id', 'タイプ' : 'dtype', '分野' : 'category',
                  '判断の理由' : 'reason', '地域' : 'region', '日付' : 'dt',
                  '景気の先行きに対する判断理由' : 'reason_future', '景気の先行き判断' : 'score_future', '景気の現状判断' : 'score_current',
                  '業種' : 'industry', '業種詳細': 'industry_detail', '職種' : 'job_title', '追加説明及び具体的状況の説明': 'comments',
                  '都道府県' : 'pref'}


for k, v in zip(keys, values):
    CONVERTER_MAP[k] = v


def to_csv(data, file=None):
    if file:
        with open(file, 'w', encoding='utf-8') as f:
            writer = csv.writer(f)
            reader = csv.reader(data.splitlines())
            for row in reader:
                writer.writerow(row)


def create_dataframe(data, date, data_type='current'):
    df = pd.read_csv(io.StringIO(data), header=7, encoding="utf-8")
    df.rename(columns={'Unnamed: 1': '都道府県'}, inplace=True)
    # remove new line and \u3000
    df['分野'] = list(map(lambda x: x.replace('\n', '').replace('\u3000', '') if type(x) == str else x, df['分野']))

    # fill nan if x[0] is digit or x is a unwanted word
    df['分野'] = list(map(lambda x: np.nan if type(x) == str and
                                            (x[0].isdigit() or x == '分野') else x, df['分野']))

    # 下記のwordが入っている行はnanとする

    if data_type == 'current':
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
    df['地域'] = list(map(lambda x: x.split('(')[1].replace(')', '') if type(x) == str else x, df['分野']))
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

    if data_type == 'current':
        df['追加説明及び具体的状況の説明'] = list(map(lambda x: x.replace('・', '') if type(x) == str else x, df['追加説明及び具体的状況の説明']))
        df = df[df['判断の理由'] != '＊']
        df = df[df['判断の理由'] != '−']
    else:
        df['景気の先行きに対する判断理由'] = list(map(lambda x: x.replace('・', '') if type(x) == str else x, df['景気の先行きに対する判断理由']))
        df = df[df['景気の先行きに対する判断理由'] != '＊']
        df = df[df['景気の先行きに対する判断理由'] != '−']

    # drop a row by condition
    df = df.dropna(thresh=4)

    # 業種・職種は別々のカラムを作ったので不要
    df = df.drop('業種・職種', 1)

    if data_type == 'current':
        df['景気の現状判断'] = list(map(lambda x: CONVERTER_MAP.get(x), df['景気の現状判断']))
    else:
        df['景気の先行き判断'] = list(map(lambda x: CONVERTER_MAP.get(x), df['景気の先行き判断']))

    if data_type == 'current':
        df['タイプ'] = '現状'
    else:
        df['タイプ'] = '先行き'

    df['日付'] = date

    # df['Id'] = generateHash(df)

    df = df.rename(columns=RENAME_COLUMNS)
    # df['id'] = generateHash(df[['dtype', 'category', 'reason', 'region', 'dt', 'reason_future', 'comments', 'industry', 'score_future', 'score_current']])
    # df = df.set_index('id')

    return df


def generateHash(df):

    hashes = []
    for idx, row in df.iterrows():
        xs = list(zip(row, row.index))
        t = tuple(tuple(x) for x in xs)

        # h = hashlib.new(t)
        r = ''.join(list(map(lambda t:str(t[0]), t)))
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


if __name__ == '__main__':

    actual_dates = ['20020212', '20020311', '20020408', '20020514', '20020610', '20020708', '20020808', '20020910',
                   '20021008', '20021111', '20021209', '20030116', '20030210', '20030310', '20030408', '20030513',
                   '20030609', '20030708', '20030808', '20030908', '20031008', '20031111', '20031208', '20040115',
                   '20040209', '20040308', '20040408', '20040514', '20040608', '20040708', '20040809', '20040908',
                   '20041008', '20041109', '20041208', '20050114', '20050208', '20050308', '20050408', '20050513',
                   '20050608', '20050708', '20050808', '20050908', '20051011', '20051109', '20051208', '20060113',
                   '20060208', '20060308', '20060410', '20060512', '20060608', '20060710', '20060808', '20060908',
                   '20061010', '20061109', '20061208', '20070112', '20070208', '20070308', '20070409', '20070510',
                   '20070608', '20070709', '20070808', '20070910', '20071009', '20071108', '20071210', '20080111',
                   '20080208', '20080310', '20080408', '20080512', '20080609', '20080708', '20080808', '20080908',
                   '20081008', '20081111', '20081208', '20090113', '20090209', '20090309', '20090408', '20090513',
                   '20090608', '20090708', '20090810', '20090908', '20091008', '20091110', '20091208', '20100112',
                   '20100208', '20100308', '20100408', '20100513', '20100608', '20100708', '20100809', '20100908',
                   '20101008', '20101109', '20101208', '20110112', '20110208', '20110308', '20110408', '20110512',
                   '20110608', '20110708', '20110808', '20110908', '20111011', '20111109', '20111208', '20120112',
                   '20120208', '20120308', '20120409', '20120510', '20120608', '20120709', '20120808', '20120910',
                   '20121009', '20121108', '20121210', '20130111', '20130208', '20130308', '20130408', '20130510',
                   '20130610', '20130708', '20130808', '20130909', '20131008', '20131111', '20131209', '20140114',
                   '20140210', '20140310', '20140408', '20140512', '20140609', '20140708', '20140808', '20140908',
                   '20141008', '20141111', '20141208', '20150113', '20150209', '20150309', '20150408', '20150513',
                   '20150608', '20150708', '20150810', '20150908', '20151008', '20151110', '20151208', '20160112',
                   '20160208', '20160308', '20160408', '20160512', '20160608', '20160708', '20160808', '20160908',
                   '20161011', '20161109', '20161208', '20170112', '20170208', '20170308', '20170410', '20170511',
                   '20170608', '20170710', '20170808', '20170908', '20171010', '20171109', '20171208', '20180112',
                   '20180208', '20180308', '20180409', '20180510',
                   '20180608', '20180709', '20180808']

    ON_HEROKU = os.environ.get("ON_HEROKU", False)

    file_path = '/tmp/%s_%s.csv' if ON_HEROKU else './%s_%s.csv'
    if not ON_HEROKU:
        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)

    DATABASE_URL = os.environ["DATABASE_URL"]

    dates = actual_dates[-100:-40]
    # dates = ['20170112']

    for dt in dates:
        today_dt = datetime.strptime(dt, '%Y%m%d')
        today = datetime.strftime(today_dt, '%Y-%m-%d')
    # today = datetime.strftime(datetime.today() - timedelta(days=1), '%Y-%m-%d')
    # today = datetime.strftime(datetime.today(), '%Y-%m-%d')

        url_dict = {
            'outlook': 'http://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher5.csv' % tuple(today.split('-')),
            'current': 'http://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher4.csv' % tuple(today.split('-'))
        }

        dfs = []
        for data_type, url in url_dict.items():
            logger.debug('doing %s' % url)

            d = retrieve_csv_file(url)
            if d:
                df = create_dataframe(d, date=today_dt, data_type=data_type)
                dfs.append(df)
                # df.to_csv('./%s_%s.csv' % (data_type, today), encoding='utf-8')
            else:
                logger.error('%s not available' % url)

        if len(dfs) < 1:
            logger.error('df is empty')

            break
        append_df = dfs[0].append(dfs[1], sort=False)
        append_df['id'] = generateHash(append_df[['dtype', 'category', 'reason', 'region', 'dt', 'reason_future', 'comments',
                                    'industry', 'score_future', 'score_current']])

        # Remove unamed column
        append_df = append_df.loc[:, ~append_df.columns.str.contains('^Unnamed')]

        # append_df['dt'] = pd.to_datetime(df['dt'])
        append_df = append_df.set_index('id')

        # concat_df = pd.concat(dfs, axis=1)

        # if ON_HEROKU:
        engine = create_engine(DATABASE_URL)
        try:
            append_df.to_sql(os.environ['BUSINESS_WATCHER_BOT_TABLE_NAME'], engine, if_exists='append')
        except Exception  as e:
            logger.error('error on insert: {0}'.format(e))
        # else:
        append_df.to_csv(file_path % ('append', today), encoding='utf-8')
        logger.debug('saving at %s' % file_path % ('append', today))