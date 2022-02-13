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