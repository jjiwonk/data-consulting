from report.SIV import directory as dr
from report.SIV import ref
import pyarrow.csv as pacsv
import pyarrow as pa
import datetime
import pandas as pd

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
    return raw_data


def apps_prep():
    df = apps_read()
    df = df.loc[df['attributed_touch_type'] == 'click']

    df['attributed_touch_time'] = df['attributed_touch_time'].fillna(df['install_time'])
    df[['attributed_touch_time', 'install_time', 'event_time']] = df[['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)
    df = df.loc[(df['install_time'] - df['attributed_touch_time']) < datetime.timedelta(days=1)]
    df = df.loc[(df['event_time'] - df['install_time']) < datetime.timedelta(days=7)]
    df = df.loc[df['is_primary_attribution'] != 'False']
    df['date'] = df['event_time'].apply(lambda x : str(x).split(' ')[0]).apply(pd.to_datetime)

    #uv 구하기
    uv_col = ['install', 're-attribution', 're-engagemaent']
    df3 = df.loc[df['event_name'].isin(uv_col)]
    df3 = df3.sort_values(by =['appsflyer_id', 'event_time'] )
    df3 = df3.drop_duplicates( 'appsflyer_id', keep= 'first')

    df3['cnt'] = int(1)
    df3 = pd.pivot_table(df3 , index= ref.columns.apps_index , columns= 'event_name', values= 'cnt',aggfunc= 'count').reset_index().fillna(0)
    apps_metric =[ 'install', 're-engagement']
    df3[apps_metric] = df3[apps_metric].astype(int)
    df3['UV(AF)'] = df3['re-attribution']  + df3['re-engagement'] + df3['install']
    df3 = df3.drop(columns= apps_metric)
    #구매와 수익 나눠서 피벗팅

    df['cnt'] = 1
    df[ref.columns.apps_index] = df[ref.columns.apps_index].astype(str).fillna('-')
    conv_piv = pd.pivot_table(df , index= ref.columns.apps_index , columns= 'event_name', values= 'cnt',aggfunc= 'sum').reset_index().fillna(0)

    reve_piv = pd.pivot_table(df, index= ref.columns.apps_index, columns='event_name', values='event_revenue', aggfunc='sum').reset_index().fillna(0)
    reve_piv = reve_piv.rename(columns = {'completed_purchase' : 'revenue', 'cancel_purchase': 'cancel_revenue' , 'first_purchase' : 'first_revenue'} )
    reve_piv = reve_piv.drop(columns=['af_complete_registration','install', 're-attribution', 're-engagement'])

    df_piv = pd.merge(conv_piv, reve_piv , on = ref.columns.apps_index , how = 'left' ).rename(columns =ref.columns.apps_rename)

    return df_piv

def apps_agg_read():
    dir = dr.report_dir + 'appsflyer_aggregated_prism'

    from_date = ref.r_date.start_date
    to_date = ref.r_date.target_date

    convert_ops = pacsv.ConvertOptions(column_types=ref.columns.apps_agg_dtype,
                                       include_columns=list(ref.columns.apps_agg_dtype.keys()))
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding='utf-8-sig')
    table_list = []

    while from_date <= to_date:  # 리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        file_name = f'appsflyer_aggregated_report_{date_name}.csv'

        table = pacsv.read_csv(dir + '/' + file_name, convert_options=convert_ops, read_options=ro)
        table_list.append(table)

        print(file_name + ' Read 완료')

        from_date = from_date + datetime.timedelta(1)

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas().fillna(0)
    return raw_data
# 읽어올때 파일 날짜 기준으로 -d7 외 값 날리기 작업 필요

df = pd.read_csv( dr.report_dir + 'appsflyer_aggregated_prism/appsflyer_aggregated_report_20221215.csv')

def apps_agg_prep():
    df = apps_agg_read()
    df = df.loc[df['agencypmd_af_prt'] == 'None']
    df = df.loc[df['media_source_pid'] == 'Facebook Ads']

    df.loc[df['conversion_type'] == 're-attribution', 're-attribution'] = df['conversions']
    df.loc[df['conversion_type'] == 're-engagement', 're-engagement'] = df['conversions']

    df[ref.columns.apps_agg_metric] = df[ref.columns.apps_agg_metric].fillna(0)
    df = df.groupby(ref.columns.apps_agg_index)[ref.columns.apps_agg_metric].sum().reset_index().fillna(0).rename(columns=ref.columns.apps_agg_rename)

    return df

#페이스북 캠페인명으로 필터링하는 조건 추가 필요

def apps_concat():
    df = apps_prep()
    df_agg = apps_prep()

    apps = pd.concat([df,df_agg])
    apps['구매(AF)'] = apps['주문'] + apps['첫구매(AF)']
    apps['매출(AF)'] = apps['주문매출'] + apps['첫구매매출(AF)']
    apps['총주문건(AF)'] = apps['구매(AF)'] + apps['주문취소(AF)']
    apps['총매출(AF)'] = apps['매출(AF)'] + apps['주문취소매출(AF)']
    apps['유입(AF)'] = apps['설치(AF)'] + apps['재설치(AF)'] + apps['리인게이지먼트']



apps.columns










