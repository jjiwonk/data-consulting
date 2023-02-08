from report.SIV import directory as dr
from report.SIV import ref
import pyarrow.csv as pacsv
import pyarrow as pa
import datetime
import pandas as pd
import urllib

def apps_read():
    dir = dr.report_dir + 'appsflyer_prism'

    from_date = ref.r_date.start_date
    to_date = ref.r_date.target_date

    convert_ops = pacsv.ConvertOptions(column_types= ref.columns.apps_dtype, include_columns= list(ref.columns.apps_dtype.keys()))
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding = 'utf-8-sig')
    table_list = []

    while from_date <= to_date : #리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        file_name = f'appsflyer_daily_report_{date_name}.csv'

        table = pacsv.read_csv(dir + '/' + file_name, convert_options=convert_ops, read_options=ro)
        table_list.append(table)

        print(file_name + ' Read 완료')

        from_date = from_date + datetime.timedelta(1)

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas()
    raw_data = raw_data.loc[raw_data['media_source'].isin(ref.columns.apps_media)]

    return raw_data

df = apps_read()

def apps_prep():
    df = apps_read()
    df = df.loc[df['attributed_touch_type'] == 'click']

    df.loc[df['media_source']== 'Apple Search Ads', 'attributed_touch_time'] = df['install_time']
    df[['attributed_touch_time', 'install_time', 'event_time']] = df[['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)
    df = df.loc[(df['install_time'] - df['attributed_touch_time']) < datetime.timedelta(days=1)]
    df = df.loc[(df['event_time'] - df['install_time']) < datetime.timedelta(days=7)]
    df = df.loc[df['is_primary_attribution'] != 'False']
    df = df.loc[df['media_source'] != 'restricted']
    df = df.loc[df['media_source'] != 'Facebook Ads']
    df['date'] = df['event_time'].apply(lambda x : str(x).split(' ')[0]).apply(pd.to_datetime)

    # 예외처리 ZONE
    df.loc[(df['media_source']== 'criteonew_int')&(df['platform'] =='android'), 'campaign'] = 'LF - ANDROID'
    df.loc[(df['media_source'] == 'criteonew_int') & (df['platform'] == 'ios'), 'campaign'] = 'LF - IOS'
    df.loc[(df['media_source'] == 'criteonew_int') & (df['campaign'] == 'LF - ANDROID'), 'ad'] = '2301_sales_catalog_SVFK0034'
    df.loc[(df['media_source'] == 'criteonew_int') & (df['campaign'] == 'LF - IOS'), 'ad'] = '2301_sales_catalog_SVCT0001'

    df.loc[(df['media_source'] == 'kakao') & (df['campaign'] == 'siv_all_m'), 'campaign'] = 'siv_all_br_main_SVFK0004'

    # 시트 활용
    c_media = ['googleadwords_int','Apple Search Ads','naver']
    g_media = ['googleadwords_int', 'Apple Search Ads']

    df.loc[df['media_source'].isin(c_media), 'campaign'] = df['campaign'].apply(lambda x: x.replace(x, ref.exc_cdict[x]) if x in ref.exc_cdict.keys() else x)
    df.loc[df['media_source'].isin(g_media), 'adset'] = df['adset'].apply(lambda x: x.replace(x, ref.exc_gdict[x]) if x in ref.exc_gdict.keys() else x)

    #키워드 구하기
    df['original_url'] = df['original_url'].apply(lambda x: urllib.parse.unquote(x))

    df['keyword'] = df['original_url'].apply(lambda x: x.split('&utm_term=')[-1] if x.find('&utm_term=') != -1 else '-')
    df.loc[df['keyword'] == '-', 'keyword'] = df['original_url'].apply(lambda x: x.split('?utm_term=')[-1] if x.find('?utm_term=') != -1 else '-')
    df['keyword'] = df['keyword'].apply(lambda x: x.split('&')[0] if x.find('&') != -1 else x )
    df.loc[df['keyword'] == '','keyword'] = '-'
    df.loc[(df['media_source'] == 'googleadwords_int') & (df['keyword'] == '-'), 'keyword'] = df['keywords']
    df.loc[(df['media_source'] == 'googleadwords_int') & (df['keyword'] == 'sivillage'), 'keyword'] = 'SIVILLAGE'

    #uv 구하기
    uv_col = ['install', 're-attribution', 're-engagement']
    df3 = df.loc[df['event_name'].isin(uv_col)]
    df3 = df3.sort_values(by =['event_time'] )
    df3 = df3.drop_duplicates( 'appsflyer_id', keep= 'first')

    df3['cnt'] = int(1)
    df3[ref.columns.apps_index] = df3[ref.columns.apps_index].astype(str).fillna('-')
    df3 = pd.pivot_table(df3 , index= ref.columns.apps_index , columns= 'event_name', values= 'cnt',aggfunc= 'count').reset_index().fillna(0)
    apps_metric =[ 'install','re-attribution','re-engagement']
    df3[apps_metric] = df3[apps_metric].astype(int)
    df3['UV(AF)'] = df3['re-attribution'] + df3['install'] + df3['re-engagement']
    df3 = df3.drop(columns= apps_metric)

    #구매와 수익 나눠서 피벗팅

    df['cnt'] = 1
    df[ref.columns.apps_index] = df[ref.columns.apps_index].astype(str).fillna('-')
    conv_piv = pd.pivot_table(df , index= ref.columns.apps_index , columns= 'event_name', values= 'cnt',aggfunc= 'sum').reset_index().fillna(0)

    reve_piv = pd.pivot_table(df, index= ref.columns.apps_index, columns='event_name', values='event_revenue', aggfunc='sum').reset_index().fillna(0)
    reve_piv = reve_piv.rename(columns = {'completed_purchase' : 'revenue', 'cancel_purchase': 'cancel_revenue' , 'first_purchase' : 'first_revenue'} )
    reve_piv = reve_piv.drop(columns=['af_complete_registration','install', 're-attribution', 're-engagement'])

    df = pd.merge(conv_piv, reve_piv , on = ref.columns.apps_index , how = 'left' )
    df = pd.merge(df, df3, on = ref.columns.apps_index, how = 'left').rename(columns =ref.columns.apps_rename).fillna(0)

    return df


def apps_agg_read():

    dir = dr.report_dir + 'appsflyer_aggregated_prism'

    from_date = ref.r_date.start_date
    to_date = ref.r_date.agg_date

    apps_agg_list =[]

    while from_date <= to_date:  # 리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        d7_date = (from_date - datetime.timedelta(6)).strftime('%Y-%m-%d')
        file_name = f'appsflyer_aggregated_report_{date_name}.csv'
        df = pd.read_csv(dir + '/' + file_name, usecols= ref.columns.apps_agg_dtype.keys())

        if from_date != to_date :
            df = df.loc[df['date']== d7_date]
        else :
            pass

        apps_agg_list.append(df)
        from_date = from_date + datetime.timedelta(1)

        print(file_name + ' Read 완료')

    apps_agg_raw = pd.concat(apps_agg_list, sort=False, ignore_index=True)
    apps_agg_raw = apps_agg_raw.loc[pd.to_datetime(apps_agg_raw['date']).dt.month == to_date.month]

    return apps_agg_raw

agg = apps_agg_read()

def apps_agg_prep():
    df = apps_agg_read()
    df['keyword'] = '-'
    df = df.loc[(df['agencypmd_af_prt'] == 'None')|(df['agencypmd_af_prt'] == 'madup')]
    df = df.loc[df['media_source_pid'] == 'Facebook Ads']

    df.index = range(len(df))
    df.loc[df['conversion_type'] == 're-attribution', 're-attribution'] = df['conversions']
    df.loc[df['conversion_type'] == 're-engagement', 're-engagement'] = df['conversions']

    df[ref.columns.apps_agg_metric] = df[ref.columns.apps_agg_metric].fillna(0)
    df = df.groupby(ref.columns.apps_agg_index)[ref.columns.apps_agg_metric].sum().reset_index().fillna(0).rename(columns=ref.columns.apps_agg_rename)

    df['UV(AF)'] = 0

    return df

def apps_concat():
    df = apps_prep()
    df_agg = apps_agg_prep()

    apps = pd.concat([df,df_agg])

    apps['구매(AF)'] = apps['주문'] + apps['첫구매(AF)']
    apps['매출(AF)'] = apps['주문매출'] + apps['첫구매매출(AF)']
    apps['총주문건(AF)'] = apps['구매(AF)'] - apps['주문취소(AF)']
    apps['총매출(AF)'] = apps['매출(AF)'] + apps['주문취소매출(AF)']
    apps['유입(AF)'] = apps['설치(AF)'] + apps['재설치(AF)'] + apps['리인게이지먼트']
    apps['appopen(AF)'] = apps['리인게이지먼트']

    apps[['브랜드구매(AF)', '브랜드매출(AF)']] = 0
    apps['구매(AF)'] = apps['주문'] + apps['첫구매(AF)']
    apps['매출(AF)'] = apps['주문매출'] + apps['첫구매매출(AF)']
    apps['총주문건(AF)'] = apps['구매(AF)'] - apps['주문취소(AF)']
    apps['총매출(AF)'] = apps['매출(AF)'] + apps['주문취소매출(AF)']
    apps['유입(AF)'] = apps['설치(AF)'] + apps['재설치(AF)'] + apps['리인게이지먼트']
    apps['appopen(AF)'] = apps['리인게이지먼트']

    apps = ref.date_dt(apps)

    apps = apps[['날짜', '매체', '캠페인', '세트', '소재', '키워드','유입(AF)','UV(AF)','appopen(AF)','구매(AF)','매출(AF)','주문취소(AF)','주문취소매출(AF)','총주문건(AF)','총매출(AF)','브랜드구매(AF)','브랜드매출(AF)','첫구매(AF)','첫구매매출(AF)','설치(AF)','재설치(AF)','가입(AF)']]
    apps = ref.adcode_mediapps(apps)

    apps.to_csv(dr.download_dir + f'appsflyer_raw/appsflyer_raw_{ref.r_date.yearmonth}.csv', index=False, encoding='utf-8-sig')

    return apps

apps = apps_concat()

apps.to_csv(dr.download_dir + f'appsflyer_raw/appsflyer_raw_{ref.r_date.yearmonth}.csv', index=False, encoding='utf-8-sig')




