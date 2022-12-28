from report.SIV import directory as dr
from report.SIV import ref
import pyarrow.csv as pacsv
import pyarrow as pa
import datetime
import pandas as pd

def ga_read():
    dir = dr.report_dir + 'ga_type1_prism'

    from_date = ref.r_date.start_date
    to_date = ref.r_date.target_date

    ga_raw = pd.DataFrame()

    while from_date <= to_date : #리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        file_name = f'127881812_type1_daily_report_{date_name}.csv'

        file = pd.read_csv(dir + '/' + file_name, usecols= ref.columns.ga1_dtype.keys())
        file['날짜'] = from_date.strftime('%Y-%m-%d')
        ga_raw = ga_raw.append(file)

        print(file_name + ' Read 완료')

        from_date = from_date + datetime.timedelta(1)

    return ga_raw

def ga_prep():
    df = ga_read()
    df = df.loc[df['﻿dataSource'] == 'web']
    app_brower = df['browser'].isin(['Safari (in-app)','Android Webview'])
    df = df[~app_brower]

    df = df.rename(columns = ref.columns.ga1_rename)
    df['코드'] = df['adContent'].apply(lambda x : x.split('_')[-1])
    return df

ga = ga_prep()


















