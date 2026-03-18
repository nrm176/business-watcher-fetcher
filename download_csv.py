import requests
from datetime import datetime

from const import HISTORICAL_DATA_BASE_PATH, REQUEST_TIMEOUT_SEC
from fetcher_shared import build_source_urls, SOURCE_CONFIG

BASE_PATH = HISTORICAL_DATA_BASE_PATH

def convert_date(target_date):
    today_dt = datetime.strptime(target_date, '%Y%m%d')
    return datetime.strftime(today_dt, '%Y-%m-%d')

def retrieve_csv_file(item, dt):
    print('getting %s' % item['url'])
    filename = '%s%s.%s.%s.csv' % (BASE_PATH, dt, item['pattern'], item['region'])
    try:
        res = requests.get(item['url'], timeout=REQUEST_TIMEOUT_SEC)
    except requests.RequestException as exc:
        print('request failed for %s: %s' % (item['url'], exc))
        return None

    if res.status_code != 200:
        return None
    else:
        with open(filename, 'wb') as file:
            for chunk in res:
                file.write(chunk)
    # data = res.content.decode('shift_jisx0213')
    # return data


def construct_urls(today):
    return build_source_urls(today, SOURCE_CONFIG)


if __name__ == '__main__':
    targets = ['20130111', '20130208', '20130308', '20130408', '20130510', '20130610', '20130708', '20130808', '20130909', '20131008', '20131111', '20131209', '20140114', '20140210', '20140310', '20140408', '20140512', '20140609', '20140708', '20140808', '20140908', '20141008', '20141111', '20141208', '20150113', '20150209', '20150309', '20150408', '20150513', '20150608', '20150708', '20150810', '20150908', '20151008', '20151110', '20151208', '20160112', '20160208', '20160308', '20160408', '20160512', '20160608', '20160708', '20160808', '20160908', '20161011', '20161109', '20161208', '20170112', '20170208', '20170308', '20170410', '20170511', '20170608', '20170710', '20170808', '20170908', '20171010', '20171109', '20171208', '20180112', '20180208', '20180308', '20180409', '20180510', '20180608', '20180709', '20180808', '20180910', '20181009', '20181108', '20181210', '20190111', '20190208']
    for target in targets:

        today = convert_date(target)
        urls = construct_urls(today)
        for url in urls:
            retrieve_csv_file(url, today)