# !pip install xlsxwriter
# !pip install pyarrow

import setting.directory as dr
import setting.report_date as rdate

import os
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import numpy as np


raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/appsflyer_prism'
result_dir = dr.download_dir
media_source = ['naver_sa_mo_main', '네이버sa', 'naversa', 'kakaosa', 'googlesa', 'naver_sa_mo_direct',
                'naversamo', 'naversapc', 'googlesamo', 'googlesapc', 'googleadwords_int']

def get_raw_df(raw_dir, required_date, media_source):
    dtypes = {
        'attributed_touch_type': pa.string(),
        'attributed_touch_time': pa.string(),
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'event_value': pa.string(),
        'event_revenue': pa.string(),
        'event_revenue_currency': pa.string(),
        'partner': pa.string(),
        'media_source': pa.string(),
        'channel': pa.string(),
        'keywords': pa.string(),
        'campaign': pa.string(),
        'campaign_id': pa.string(),
        'adset': pa.string(),
        'adset_id': pa.string(),
        'ad': pa.string(),
        'ad_id': pa.string(),
        'site_id': pa.string(),
        'sub_site_id': pa.string(),
        'sub_param_1': pa.string(),
        'sub_param_2': pa.string(),
        'sub_param_3': pa.string(),
        'sub_param_4': pa.string(),
        'sub_param_5': pa.string(),
        'appsflyer_id': pa.string(),
        'advertising_id': pa.string(),
        'idfa': pa.string(),
        'customer_user_id': pa.string(),
        'imei': pa.string(),
        'idfv': pa.string(),
        'platform': pa.string(),
        'is_retargeting': pa.string(),
        'is_primary_attribution': pa.string(),
        'original_url': pa.string(),
        'keyword_id': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    date_check = required_date.strftime('%Y%m')
    start_date = required_date.replace(day=1).strftime('%Y%m%d')
    end_date = required_date.strftime('%Y%m%d')

    files = os.listdir(raw_dir)
    files = [f for f in files if '.csv' in f and str(f)[-12:-6] == date_check]
    raw_files = [f for f in files if
                 (int(str(f)[-12:-4]) >= int(start_date)) & (int(str(f)[-12:-4]) <= int(end_date))]

    for f in raw_files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    raw_df.media_source = raw_df.media_source.str.lower()
    raw_df = raw_df.loc[raw_df['media_source'].isin(media_source)]
    return raw_df


def prep_data(raw_df):
    prep_df = raw_df
    prep_df[['attributed_touch_time','install_time','event_time']] = prep_df[['attributed_touch_time','install_time','event_time']].apply(pd.to_datetime)
    prep_df['event_date'] = pd.to_datetime(prep_df['event_time']).dt.date

    # install 데이터 가공 :: event_name in ['install','re-attribution','re-engagement']
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['install','re-attribution','re-engagement']))
                                           &(prep_df['media_source']=='naversamo')&(prep_df['event_date'] == '2022-08-02')].index)
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['install','re-attribution','re-engagement']))
                                            &(prep_df['media_source']=='googleadwords_int')&(~prep_df['channel'].isin(['Search']))].index)
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['install','re-attribution','re-engagement']))
                                             &(prep_df['attributed_touch_type']!='click')].index)
    prep_df['CTIT'] = (prep_df['install_time'] - prep_df['attributed_touch_time']).apply(lambda x: x.total_seconds()/(60*60*24))
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['install','re-attribution','re-engagement']))
                                             &(prep_df['CTIT'] > 7)].index)

    # in-app events 데이터 가공 :: event_name in ['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                                           &(prep_df['media_source']=='naversamo')&(prep_df['event_date'] == '2022-08-02')].index)
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                                             & (prep_df['media_source'] == 'googleadwords_int') & (~prep_df['channel'].isin(['Search']))].index)
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                                             & (prep_df['attributed_touch_type'] != 'click')].index)
    prep_df = prep_df.drop(
        index=prep_df.loc[(prep_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                          & (prep_df['CTIT'] > 7)].index)
    prep_df['ITET'] = (prep_df['event_time'] - prep_df['install_time']).apply(lambda x: x.total_seconds()/(60*60*24))
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                                             &(prep_df['ITET'] > 30)].index)

    prep_df.loc[prep_df['keywords'].isin(['', '{keyword}']),'keywords'] = '-'
    prep_df.loc[prep_df['campaign'] == '', 'campaign'] = '-'
    prep_df.loc[prep_df['adset'] == '', 'adset'] = '-'
    prep_df.loc[prep_df['ad'] == '', 'ad'] = '-'

    prep_df.loc[prep_df['ad'] == 'utm_content', 'keywords'] = prep_df['adset']
    prep_df.loc[prep_df['ad'] == 'utm_content', ['adset', 'ad']] = '-'
    prep_df.loc[(prep_df['ad'] == 'bridge_finda-brand-2_2207')&(prep_df['adset'] == '대출'), 'keywords'] = prep_df['adset']
    prep_df.loc[(prep_df['ad'] == 'bridge_finda-brand-2_2207') & (prep_df['adset'] == '대출'), ['adset', 'ad']] = '-'
    # 추가 예외처리

    return prep_df


def download_df(prep_df, required_date, result_dir):
    date = required_date.strftime('%m%d')
    writer = pd.ExcelWriter(result_dir + f'/종합_{date}.xlsx', engine='xlsxwriter', engine_kwargs={'options':{'strings_to_urls': False}})

    # 데이터 구분
    loan_total = prep_df.loc[prep_df['event_name']=='loan_contract_completed'].reset_index(drop=True)
    loan_total.loc[:,'event_name'] = 'loan_contract_completed TOTAL'
    install_total = prep_df.loc[prep_df['event_name'].isin(['install','re-engagement','re-attribution'])].reset_index(drop=True)
    prep_unique = prep_df.sort_values(by='event_time').drop_duplicates(['is_retargeting', 'event_name', 'appsflyer_id'], keep='first')
    event_total = prep_unique.loc[prep_unique['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view'])]
    event_total = pd.concat([event_total, loan_total], axis=0).reset_index(drop=True)

    install_summary = install_total[['attributed_touch_type', 'event_date', 'media_source', 'keywords', 'ad', 'campaign', 'adset', 'is_retargeting', 'event_time']]
    install_summary['is_retargeting'] = install_summary['is_retargeting'].apply(lambda x: 'RE' if x == 'True' else 'UA')
    install_summary = install_summary.rename(columns={'is_retargeting':'ua/re'})
    event_summary = event_total[['attributed_touch_type', 'event_date', 'media_source', 'keywords', 'ad', 'campaign', 'adset', 'event_name', 'is_retargeting', 'event_time', 'attributed_touch_time', 'ITET']]
    event_summary['is_retargeting'] = event_summary['is_retargeting'].apply(lambda x: 'RE' if x == 'True' else 'UA')
    event_summary['차이(date)'] = event_summary['ITET'].astype(np.int)
    event_summary = event_summary.rename(columns={'is_retargeting': 'ua/re','ITET':'차이(time)'})

    # 데이터 출력
    install_summary.to_excel(writer, sheet_name='install(summary)', index=False)
    event_summary.to_excel(writer, sheet_name='event(summary)', index=False)
    install_total.to_excel(writer, sheet_name='install(total)', index=False)
    event_total.to_excel(writer, sheet_name='event(total)', index=False)

    writer.close()
    print('download success')


required_date = rdate.day_1
raw_df = get_raw_df(raw_dir, required_date, media_source)
prep_df = prep_data(raw_df)


download_df(prep_df, required_date, result_dir)
