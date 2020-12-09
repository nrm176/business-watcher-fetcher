import requests
from datetime import datetime, timedelta

BASE_PATH = './historical_data/'

def convert_date(target_date):
    dt_str = target_date
    today_dt = datetime.strptime(dt_str, '%Y%m%d')
    today = datetime.strftime(today_dt, '%Y-%m-%d')
    return today

def retrieve_csv_file(item, dt):
    print('getting %s' % item['url'])
    filename = '%s%s.%s.%s.csv' % (BASE_PATH, dt, item['pattern'], item['region'])
    res = requests.get(item['url'])
    if res.status_code != 200:
        return None
    else:
        with open(filename, 'wb') as file:
            for chunk in res:
                file.write(chunk)
    # data = res.content.decode('shift_jisx0213')
    # return data


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


if __name__ == '__main__':
    targets = ['20130111', '20130208', '20130308', '20130408', '20130510', '20130610', '20130708', '20130808', '20130909', '20131008', '20131111', '20131209', '20140114', '20140210', '20140310', '20140408', '20140512', '20140609', '20140708', '20140808', '20140908', '20141008', '20141111', '20141208', '20150113', '20150209', '20150309', '20150408', '20150513', '20150608', '20150708', '20150810', '20150908', '20151008', '20151110', '20151208', '20160112', '20160208', '20160308', '20160408', '20160512', '20160608', '20160708', '20160808', '20160908', '20161011', '20161109', '20161208', '20170112', '20170208', '20170308', '20170410', '20170511', '20170608', '20170710', '20170808', '20170908', '20171010', '20171109', '20171208', '20180112', '20180208', '20180308', '20180409', '20180510', '20180608', '20180709', '20180808', '20180910', '20181009', '20181108', '20181210', '20190111', '20190208']
    for target in targets:

        today = convert_date(target)
        urls = construct_urls(today)
        for url in urls:
            retrieve_csv_file(url, today)