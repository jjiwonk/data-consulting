import os
# !pip install xlsxwriter
import setting.directory as dr
import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

def get_raw_df(raw_dir, required_date):
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

    files = os.listdir(raw_dir)
    files = [f for f in files if '.csv' in f]

    given_date = datetime.datetime.strptime(required_date,'%Y-%m-%d')
    start_date = given_date.replace(day=1).strftime('%Y%m%d')
    end_date = given_date.strftime('%Y%m%d')
    raw_files = [f for f in files if
                 (int(str(f)[-12:-4]) >= int(start_date)) & (int(str(f)[-12:-4]) <= int(end_date))]

    for f in raw_files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()

    raw_df.media_source = raw_df.media_source.str.lower()
    raw_df = raw_df.loc[((raw_df['platform']=='android')&(raw_df['media_source']
                                                         .isin(['naver_sa_mo_main', '네이버sa', 'naversa', 'kakaosa',
                                                                'googlesa', 'naver_sa_mo_direct', 'naversamo', 'naversapc',
                                                                'googlesamo', 'googlesapc', 'googleadsword_int'])))|
                        ((raw_df['platform']=='ios')&(raw_df['media_source']
                                                         .isin(['naver_sa_mo_main', '네이버sa', 'naversa', 'kakaosa',
                                                                'googlesa', 'naversamo', 'naversapc',
                                                                'googlesamo', 'googlesapc', 'googleadsword_int'])))]
    return raw_df

def prep_data(raw_df):
    raw_df[['attributed_touch_time','install_time','event_time']] = raw_df[['attributed_touch_time','install_time','event_time']].apply(pd.to_datetime)
    raw_df['event_date'] = raw_df['event_time'].apply(lambda x: x.date()).apply(str)

    # install 데이터 가공 :: event_name in ['install','re-attribution','re-engagement']
    prep_df = raw_df.drop(index=raw_df.loc[(raw_df['event_name'].isin(['install','re-attribution','re-engagement']))
                                           &(raw_df['media_source']=='naversamo')&(raw_df['event_date'] == '2022-08-02')].index)
    prep_df = prep_df.drop(columns='event_date')
    prep_df = prep_df.drop(index=prep_df.loc[(raw_df['event_name'].isin(['install','re-attribution','re-engagement']))
                                            &(prep_df['media_source']=='googleadwords_int')&(~prep_df['channel'].isin(['ACI_Search','ACE_Search']))].index)
    prep_df = prep_df.drop(index=prep_df.loc[(raw_df['event_name'].isin(['install','re-attribution','re-engagement']))
                                             &(prep_df['attributed_touch_type']!='click')].index)
    prep_df['CTIT'] = (prep_df['install_time'] - prep_df['attributed_touch_time']).apply(lambda x: x.days)
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['install','re-attribution','re-engagement']))
                          &(prep_df['CTIT'] > 7)].index)

    # in-app events 데이터 가공 :: event_name in ['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']
    prep_df = prep_df.drop(index=prep_df.loc[(raw_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                                             & (prep_df['media_source'] == 'googleadwords_int') & (
                                                 ~prep_df['channel'].isin(['ACI_Search', 'ACE_Search']))].index)
    prep_df = prep_df.drop(index=prep_df.loc[(raw_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                                             & (prep_df['attributed_touch_type'] != 'click')].index)
    prep_df = prep_df.drop(
        index=prep_df.loc[(prep_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                          & (prep_df['CTIT'] > 7)].index)
    prep_df['ITET'] = (prep_df['event_time'] - prep_df['install_time']).apply(lambda x: x.days)
    prep_df = prep_df.drop(index=prep_df.loc[(prep_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view']))
                                             &(prep_df['ITET'] > 30)].index)

    prep_df = prep_df.drop(columns=['CTIT','ITET'])

    return prep_df

def download_df(prep_df, required_date, result_dir):
    date = datetime.datetime.strptime(required_date, '%Y-%m-%d').strftime('%m%d')
    writer = pd.ExcelWriter(result_dir + f'/종합_{date}.xlsx', engine='xlsxwriter', engine_kwargs={'options':{'strings_to_urls': False}})

    loan_total = prep_df.loc[prep_df['event_name']=='loan_contract_completed'].reset_index(drop=True)
    loan_total.loc[:,'event_name'] = 'loan_contract_completed TOTAL'
    loan_total.to_excel(writer, sheet_name='event(대출실행_TOTAL)', index=False)

    prep_unique = prep_df.sort_values(by='event_time').drop_duplicates(['event_name', 'appsflyer_id'], keep='first')
    install_total = prep_unique.loc[prep_df['event_name'].isin(['install','re-engagement','re-attribution'])].reset_index(drop=True)
    install_total.to_excel(writer, sheet_name='install(total)', index=False)
    event_total = prep_unique.loc[prep_df['event_name'].isin(['Viewed LA Home','Clicked Signup Completion Button','loan_contract_completed','MD_complete_view'])]
    event_total = pd.concat([event_total, loan_total], axis=0).reset_index(drop=True)
    event_total.to_excel(writer, sheet_name='event(total)', index=False)

    ua_install = install_total.loc[install_total['is_retargeting']=='False'].reset_index(drop=True)
    re_install = install_total.loc[install_total['is_retargeting']=='True'].reset_index(drop=True)
    ua_install.to_excel(writer, sheet_name='install(UA)', index=False)
    re_install.to_excel(writer, sheet_name='install(RE)', index=False)

    ua_event = event_total.loc[event_total['is_retargeting']=='False'].reset_index(drop=True)
    re_event = event_total.loc[event_total['is_retargeting']=='True'].reset_index(drop=True)
    ua_event.to_excel(writer, sheet_name='event(UA)', index=False)
    re_event.to_excel(writer, sheet_name='event(RE)', index=False)

    writer.close()
    print('download success')


raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/appsflyer_prism'
result_dir = dr.download_dir
# yyyy-mm-dd 형식으로 입력
required_date = '2022-09-06'

raw_df = get_raw_df(raw_dir, required_date)
prep_df = prep_data(raw_df)
download_df(prep_df, required_date, result_dir)

