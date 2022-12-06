from spreadsheet import spreadsheet
import datetime

doc = spreadsheet.spread_document_read(
    'https://docs.google.com/spreadsheets/d/1BRyTV3FEnRFJWvyP7sMsJopwDkqF8UGF8ZxrYvzDOf0/edit#gid=2141730654')

info_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 3).reset_index(drop=True)
setting_df = spreadsheet.spread_sheet(doc, '매체 전처리', 0, 0).reset_index(drop=True)

class report_date :
    target_date = datetime.datetime.strptime(setting_df['리포트 기준 날짜'][0], '%Y-%m-%d')
    yearmonth = target_date.strftime('%Y%m')


class columns :
    temp_cols = []
    dimension_cols = ['매체']
    metric_cols = []

    for k, v in enumerate(info_df.iloc[0]):
        k = info_df.columns[k]
        if v == 'temp':
            temp_cols.append(k)
        elif v == 'dimension':
            dimension_cols.append(k)
        elif v == 'metric':
            metric_cols.append(k)

    result_columns = dimension_cols + metric_cols
    read_columns = temp_cols + result_columns


item_list = ['read', 'prep', 'temp', 'dimension', 'metric']

info_dict = {}

for media in info_df['매체'][1:].to_list() :
    info_dict[media] = {}
    for item in item_list :
        item_df = info_df.loc[info_df['매체'].isin(['type',media])]
        item_df = item_df.set_index('매체').transpose()
        item_df = item_df.loc[item_df['type']==item]
        item_df = item_df.loc[item_df[media].str.len()>0]

        item_dict = dict(zip(item_df.index, item_df[media]))

        info_dict[media][item] = item_dict