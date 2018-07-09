import pandas as pd
import re
from datetime import datetime, timedelta
import numpy as np
import requests
import io
import csv

keys = ['◎', '○', '□', '▲', '×']
values = [1.0, 0.75, 0.5, 0.25, 0.0]
CONVERTER_MAP = {}

for k, v in zip(keys, values):
    CONVERTER_MAP[k] = v
print(CONVERTER_MAP)

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

    # if data_type == '現状':
    #     df = df[df['判断の理由'] != '＊']
    #     df = df[df['判断の理由'] != '−']
    # else:
    #     df = df[df['景気の先行きに対する判断理由'] != '＊']
    #     df = df[df['景気の先行きに対する判断理由'] != '−']


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


    df['Id']= generateHash(df)

    df = df.set_index('Id')

    return df

def generateHash(df):
    hashes = []
    for idx, row in df.iterrows():
        xs = list(zip(row, row.index))
        t = tuple(tuple(x) for x in xs)
        hashes.append(hash(t))
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

    # today = datetime.strftime(datetime.today() + timedelta(days=1), '%Y-%m-%d')
    today = datetime.strftime(datetime.today(), '%Y-%m-%d')

    url_dict = {
        'outlook' : 'http://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher5.csv' % tuple(today.split('-')),
        'current' : 'http://www5.cao.go.jp/keizai3/%s/%s%swatcher/watcher4.csv' % tuple(today.split('-'))
    }

    for data_type, url in url_dict.items():
        d = retrieve_csv_file(url)
        if d:
            df = create_dataframe(d, date=today, data_type=data_type)
            df.to_csv('./%s_%s.csv' % (data_type, today), encoding='utf-8')
        else:
            print('%s not available' % url)