import setting.directory as dr
import setting.report_date as rdate
from setting.info import tableau_info
from spreadsheet import spreadsheet

import pyarrow as pa
import pyarrow.csv as pacsv
import pandas as pd
import os
import numpy as np

# 실행 전 확인 필요사항
# 1) setting.report_date 내 from_date, to_date 확인
# 2) setting.info 내 tableau_info 확인


prep_doc = spreadsheet.spread_document_read('https://docs.google.com/spreadsheets/d/1_-jgnU51smApXKQ4R8n0ek984s-4NQ1TfQefv_SbQKE/edit#gid=0')

def get_custom_sheet(doc, sheet_name):
    # 태블로 대시보드 컬럼 설정 불러오기
    sheet_data = spreadsheet.spread_sheet(doc, sheet_name)

    return sheet_data


def get_raw_df(apps_dir, is_organic):
    if is_organic == False:
        dtypes = {
            'attributed_touch_time': pa.string(),
            'attributed_touch_type': pa.string(),
            'install_time': pa.string(),
            'event_time': pa.string(),
            'event_name': pa.string(),
            'media_source': pa.string(),
            'adset': pa.string(),
            'ad': pa.string(),
            'campaign': pa.string(),
            'appsflyer_id': pa.string(),
            'is_retargeting': pa.string(),
            'event_value': pa.string(),
            'platform': pa.string()
        }
    else:
        dtypes = {
            'Attributed Touch Time': pa.string(),
            'Attributed Touch Type': pa.string(),
            'Install Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'Media Source': pa.string(),
            'Campaign': pa.string(),
            'AppsFlyer ID': pa.string(),
            'Is Retargeting': pa.string(),
            'Event Value': pa.string(),
            'Platform': pa.string()
        }

    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    files = os.listdir(apps_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(apps_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    table = pa.concat_tables(table_list)
    raw_df = table.to_pandas()
    if is_organic == False:
        raw_df['is_organic'] = False
    else:
        raw_df = raw_df.rename(columns=
                               {'Attributed Touch Time': 'attributed_touch_time',
                                'Attributed Touch Type': 'attributed_touch_type',
                                'Install Time': 'install_time',
                                'Event Time': 'event_time',
                                'Event Name': 'event_name',
                                'Media Source': 'media_source',
                                'Campaign': 'campaign', 'AppsFlyer ID': 'appsflyer_id',
                                'Is Retargeting': 'is_retargeting',
                                'Event Value': 'event_value',
                                'Platform': 'platform'})
        raw_df['is_organic'] = True

    return raw_df


### finda 추가 필터링 조건 ###
def finda_paid_prep(raw_df):
    raw_df.loc[raw_df['attributed_touch_type']!='click', 'is_organic'] = True
    raw_df.loc[(raw_df['platform']=='ios')&(raw_df['attributed_touch_time']==''), 'is_organic'] = False

    # 모비데이즈 데이터 예외처리
    raw_df_copy = raw_df.copy()
    raw_df_copy = raw_df_copy.loc[raw_df_copy['media_source']!='']
    raw_df_copy_dedup = raw_df_copy[['media_source', 'campaign', 'adset', 'ad']].drop_duplicates(['campaign', 'adset', 'ad'])
    raw_df_copy_dedup = raw_df_copy_dedup.rename(columns = {'media_source' : 'media_source_find'})

    raw_df_merge = raw_df.merge(raw_df_copy_dedup, on = ['campaign', 'adset', 'ad'], how = 'left')

    raw_df_merge.loc[raw_df_merge['media_source']=='', 'media_source'] = raw_df_merge['media_source_find']

    del raw_df_merge['media_source_find']
    return raw_df_merge
############################


def raw_data_concat(apps_paid_dir, apps_organic_dir, media_filter, from_date='', to_date=''):
    if (apps_paid_dir != dr.dropbox_dir)&(apps_organic_dir != dr.dropbox_dir):
        paid_df = get_raw_df(apps_paid_dir, False)
        organic_df = get_raw_df(apps_organic_dir, True)
        ### finda 추가 필터링 ###
        paid_df = finda_paid_prep(paid_df)
        #######################
        raw_df = pd.concat([paid_df, organic_df], sort=False, ignore_index=True)
    elif (apps_paid_dir != dr.dropbox_dir)&(apps_organic_dir == dr.dropbox_dir):
        raw_df = get_raw_df(apps_paid_dir, False)
        ### finda 추가 필터링 ###
        raw_df = finda_paid_prep(raw_df)
        #######################
    elif (apps_paid_dir == dr.dropbox_dir)&(apps_organic_dir != dr.dropbox_dir):
        raw_df = get_raw_df(apps_organic_dir, True)
    else:
        raise Exception('드롭박스 내 appsflyer 데이터 위치를 입력 해주세요.')

    raw_df['is_retargeting'] = raw_df['is_retargeting'].str.lower()

    raw_df[['attributed_touch_time', 'install_time', 'event_time']] = raw_df[
        ['attributed_touch_time', 'install_time', 'event_time']].apply(pd.to_datetime)

    raw_df['click_date'] = raw_df['attributed_touch_time'].dt.date
    raw_df['click_hour'] = raw_df['attributed_touch_time'].dt.hour
    raw_df['click_weekday'] = raw_df['attributed_touch_time'].dt.weekday
    raw_df['event_date'] = raw_df['event_time'].dt.date
    raw_df['event_hour'] = raw_df['event_time'].dt.hour
    raw_df['event_weekday'] = raw_df['event_time'].dt.weekday
    raw_df['CTET'] = raw_df['event_time'] - raw_df['attributed_touch_time']

    raw_df = raw_df.sort_values(['event_time','attributed_touch_time'])
    raw_df.index = range(0, len(raw_df))

    raw_df_dedup = raw_df.drop_duplicates(['event_time', 'event_name', 'appsflyer_id'], keep='last')
    raw_df_dedup = raw_df_dedup.loc[~(raw_df_dedup['media_source'].isin(media_filter))]

    if from_date == '':
        from_date = np.min(raw_df_dedup['event_date'])
    if to_date == '':
        to_date = np.max(raw_df_dedup['event_date'])
    raw_df_dedup = raw_df_dedup.loc[
        (raw_df_dedup['event_date'] >= from_date) & (raw_df_dedup['event_date'] <= to_date)]
    return raw_df_dedup


def campaign_name_exception(prep_doc, raw_df):
    ### finda 추가 필터링 조건 ###
        # 캠페인명 수정
    raw_df.loc[raw_df['media_source'] != 'moloco_int', 'campaign'] = raw_df['campaign'].apply(lambda x: x.replace('MobidaysAgency_', 'Madit_'))
    raw_df.loc[raw_df['media_source'] != 'moloco_int', 'campaign'] = raw_df['campaign'].apply(lambda x: x.replace('Mobi_', 'Madit_'))
    raw_df.loc[raw_df['media_source'] == 'cauly_int', 'campaign'] = raw_df['campaign'].apply(lambda x: 'Madit_CAULY_LOAN_RT_AOS_CPC_220714' if x.startswith('99') else x)
    raw_df.loc[raw_df['media_source'] == 'tnk_int', 'campaign'] = 'Madit_TNK_LOAN_NU_AOS_REWARD-CPA_220715'
        # 특정 캠페인 구분 organic으로 변경
    raw_df.loc[(raw_df['media_source'] == 'kakao_int')&(raw_df['campaign']=='kakao_biz_loancontract'), 'is_organic'] = True
    raw_df.loc[(raw_df['media_source'] == 'kakao')&(raw_df['campaign'] != 'talk'), 'is_organic'] = True
    ############################

    campaign_mapping = get_custom_sheet(prep_doc, tableau_info.account_name)
    media_source = campaign_mapping.media_source.unique()
    campaign_dict = {}
    for source in media_source:
        campaign_temp = campaign_mapping.loc[campaign_mapping['media_source']==source][['before_campaign','after_campaign']].set_index('before_campaign').to_dict()
        campaign_dict[source] = campaign_temp['after_campaign']

    for media in campaign_dict.keys():
        raw_df.loc[raw_df['media_source'] == media, 'campaign'] = raw_df['campaign'].apply(
        lambda x: campaign_dict[media][x] if x in campaign_dict[media].keys() else x)

    return raw_df


def sankey_data_prep(prep_doc):
    sheet_data = get_custom_sheet(prep_doc, '가공조건')
    info_data = sheet_data.loc[sheet_data['광고주'] == tableau_info.account_name]

    apps_paid_dir = dr.dropbox_dir + info_data['paid_raw 데이터 위치'].iloc[0]
    apps_organic_dir = dr.dropbox_dir + info_data['organic_raw 데이터 위치'].iloc[0]
    if info_data['제외 매체'].iloc[0] == '':
        media_filter = []
    else:
        media_filter = info_data['제외 매체'].iloc[0].strip().replace(', ',',').split(',')
    raw_df = raw_data_concat(apps_paid_dir, apps_organic_dir, media_filter=media_filter, from_date = rdate.from_date, to_date = rdate.to_date)

    if info_data['캠페인명 가공여부'].iloc[0] == 'TRUE':
        raw_df_exception = campaign_name_exception(prep_doc, raw_df)
    else:
        raw_df_exception = raw_df

    target_event_list = info_data['타겟 이벤트'].iloc[0].strip().replace(', ',',').split(',')
    event_data = raw_df_exception.loc[raw_df_exception['event_name'].isin(target_event_list)]

    event_data.loc[pd.isnull(event_data['attributed_touch_time']), 'attributed_touch_time'] = event_data['install_time']
    event_data['click_date'] = event_data['attributed_touch_time'].dt.date
    event_data['click_weekday'] = event_data['attributed_touch_time'].dt.weekday
    event_data = event_data.loc[event_data['attributed_touch_type']!='impression']


    event_data['CTET'] = (event_data['event_time'] - event_data['attributed_touch_time']).apply(
        lambda x: x.total_seconds() / 86400)
    event_data['CTIT'] = (event_data['install_time'] - event_data['attributed_touch_time']).apply(
        lambda x: x.total_seconds() / 86400)
    event_data['ITET'] = (event_data['event_time'] - event_data['install_time']).apply(lambda x: x.total_seconds() / 86400)

    ctet_limit = info_data['CTET 기여기간'].iloc[0]
    ctit_limit = info_data['CTIT 기여기간'].iloc[0]
    itet_limit = info_data['ITET 기여기간'].iloc[0]

    if ctet_limit != '':
        event_data = event_data.loc[event_data['CTET'] < int(ctet_limit)]
    if ctit_limit != '':
        event_data = event_data.loc[event_data['CTIT'] < int(ctit_limit)]
    if itet_limit != '':
        event_data = event_data.loc[event_data['ITET'] < int(itet_limit)]

    event_data = event_data.loc[~event_data['media_source'].isin(['', 'restricted'])]
    event_data['Cnt'] = 1
    event_data['click_month'] = event_data['attributed_touch_time'].dt.month
    event_data['event_month'] = event_data['event_time'].dt.month
    event_data['UA / RE'] = event_data['is_retargeting'].apply(lambda x : 'RE' if x == 'true' else 'UA')

    event_data_filtered = event_data[['UA / RE', 'media_source', 'click_month', 'click_date', 'click_weekday',
                                      'event_month','event_date','event_weekday',
                                      'event_name','platform','appsflyer_id','Cnt']]
    event_data_filtered = event_data_filtered.loc[event_data_filtered['click_date']>=rdate.from_date]

    event_data_pivot = event_data_filtered.pivot_table(index=['UA / RE', 'media_source', 'click_month', 'click_date', 'click_weekday',
                                                              'event_month','event_date','event_weekday','platform','event_name'],
                                                       aggfunc='sum').reset_index()


    event_data_pivot = event_data_pivot.sort_values(['event_month','click_month', 'event_weekday','click_weekday'])

    weekday_dict = {0: '월요일',
                    1: '화요일',
                    2: '수요일',
                    3: '목요일',
                    4: '금요일',
                    5: '토요일',
                    6: '일요일'}
    event_data_pivot['click_weekday'] = event_data_pivot['click_weekday'].apply(lambda x: weekday_dict.get(x))
    event_data_pivot['event_weekday'] = event_data_pivot['event_weekday'].apply(lambda x: weekday_dict.get(x))

    event_data_pivot['click_month'] = event_data_pivot['click_month'].apply(lambda x: str(x) + "월")
    event_data_pivot['event_month'] = event_data_pivot['event_month'].apply(lambda x: str(x) + "월")
    event_data_pivot.to_excel(dr.download_dir + f'/{tableau_info.result_name}_event_flow_data.xlsx', index=False, encoding='utf-8-sig')


sankey_data_prep(prep_doc)