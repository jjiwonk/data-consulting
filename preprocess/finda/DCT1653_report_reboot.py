from setting import directory as dr
import os
import pandas as pd
import numpy as np
import copy

class metric_store():
    def __init__(self):
        self.attribution_window_dict = {'D0': 1, 'D7': 7.5, 'D30': 30}
        self.attribution_window = list(self.attribution_window_dict.keys())

        self.metric_event_dict = {
            'Cnt': ['install',
                    'loan_contract_completed',
                    'loan_contract_completed_fee',
                    'Clicked Signup Completion Button',
                    're-attribution',
                    're-engagement',
                    'Viewed LA Home',
                    'Viewed LA Home No Result'],
            'event_revenue': ['loan_contract_completed_fee']
        }

        target_event = set()
        for event_list in self.metric_event_dict.values():
            target_event = target_event | set(event_list)

        self.target_event = list(target_event)
        self.metric_group = {}

    def initialize_metric_hierarchy(self):
        metric_hierarchy = {}

        for metric_type, events in self.metric_event_dict.items():
            metric_hierarchy[metric_type] = {}
            for event in events:
                metric_hierarchy[metric_type][event] = {}
                for window in self.attribution_window_dict.keys():
                    metric_hierarchy[metric_type][event][window] = []

        self.metric_hierarchy = metric_hierarchy

    def update_metric_hierarchy(self, metric_group_name, metric_type, event, window, val, metric_name):
        self.metric_hierarchy[metric_type][event][window].append(val)

        metric_col_name = f'{metric_type}/{event}/{window}/{val}'

        if metric_group_name not in self.metric_group.keys() :
            self.metric_group[metric_group_name] = {}

        self.metric_group[metric_group_name][metric_col_name] = metric_name

    def get_metric_list(self, target_metric_group, search_type, search_value):
        search_key_dict = {
            'metric_type' : 0,
            'event' : 1,
            'window' : 2,
            'date_type' : 3}
        search_key = search_key_dict.get(search_type)

        target_metric_list = self.metric_group[target_metric_group].keys()
        return_metric_list = [col for col in target_metric_list if col.split('/')[search_key]==search_value]

        return return_metric_list

# run metric store
ms = metric_store()
ms.initialize_metric_hierarchy()

# 지표 업데이트 (측정값 그룹 / 측정값 유형 / 이벤트 / 기여기간 / 귀속 구분 / 측정값 이름)
ms.update_metric_hierarchy('cn','Cnt', 'install', 'D30', 'event_date', 'install(cn) D30')
ms.update_metric_hierarchy('cn','event_revenue', 'loan_contract_completed_fee', 'D30', 'event_date', 'REVENUE(cn) D30')
ms.update_metric_hierarchy('cn','Cnt', 'loan_contract_completed', 'D0', 'click_date', 'LOAN(cn) D0.T')
ms.update_metric_hierarchy('cn','Cnt', 'loan_contract_completed_fee', 'D0', 'click_date', 'LOANFEE(cn) D0.T')
ms.update_metric_hierarchy('cn','event_revenue', 'loan_contract_completed_fee', 'D0', 'click_date', 'REVENUE(cn) D0.T')
ms.update_metric_hierarchy('cn','Cnt', 'loan_contract_completed', 'D30', 'click_date', 'LOAN(cn) D30.T')
ms.update_metric_hierarchy('cn','Cnt', 'loan_contract_completed_fee', 'D30', 'click_date', 'LOANFEE(cn) D30.T')
ms.update_metric_hierarchy('cn','event_revenue', 'loan_contract_completed_fee', 'D30', 'click_date', 'REVENUE(cn) D30.T')
ms.update_metric_hierarchy('cn','Cnt', 'loan_contract_completed', 'D7', 'click_date', 'LOAN(cn) D7.T')
ms.update_metric_hierarchy('cn','Cnt', 'loan_contract_completed_fee', 'D7', 'click_date', 'LOANFEE(cn) D7.T')
ms.update_metric_hierarchy('cn','event_revenue', 'loan_contract_completed_fee', 'D7', 'click_date', 'REVENUE(cn) D7.T')
ms.update_metric_hierarchy('uni','Cnt', 'Clicked Signup Completion Button', 'D30', 'event_date', 'CS(uni) D30')
ms.update_metric_hierarchy('uni','Cnt', 'Clicked Signup Completion Button', 'D0', 'click_date', 'CS(uni) D0.T')
ms.update_metric_hierarchy('uni','Cnt', 'install', 'D0', 'click_date', 'install(uni) D0.T')
ms.update_metric_hierarchy('uni','Cnt', 're-attribution', 'D0', 'click_date', 're-attribution(uni) D0.T')
ms.update_metric_hierarchy('uni','Cnt', 're-engagement', 'D0', 'click_date', 're-engagement(uni) D0.T')

ms.update_metric_hierarchy('uni','Cnt', 'Viewed LA Home', 'D30', 'click_date', 'VLH(uni) D30.T')
ms.update_metric_hierarchy('uni','Cnt', 'Viewed LA Home No Result', 'D30', 'click_date', 'VLHN(uni) D30.T')

ms.update_metric_hierarchy('uni_vlh','Cnt', 'Viewed LA Home', 'D0', 'click_date', 'VLH(uni) D0.T')
ms.update_metric_hierarchy('uni_vlh','Cnt', 'Viewed LA Home No Result', 'D0', 'click_date', 'VLHN(uni) D0.T')

# data_set
data_set = pd.read_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/핀다/DCT-1631/data_set.csv')
data_set['install_time'] = pd.to_datetime(data_set['install_time'])
data_set['event_time'] = pd.to_datetime(data_set['event_time'])
data_set['attributed_touch_time'] = pd.to_datetime(data_set['attributed_touch_time'])

source_list = ['KA-FRIEND', 'kakao_int', 'googleadwords_int', 'Apple Search Ads', 'moloco_int', 'rtbhouse_int', 'appier_int',
        'cauly_int', 'nstation_int', 'nswitch_int', 'adisonofferwall_int', 'cashfriends_int', 'bobaedream', 'v3', 'remember',
        'encar', 'bytedanceglobal_int', 'naversd', 'tnk_int', 'toss', 'naver_int', 'blind', 'criteonew_int',
        'igaworkstradingworksvideo_int', 'inmobidsp_int', 'jobplanet_onelink', 'naverband', 'doohub_int', 'remerge_int'
        ,'igaworkstradingworks_int','xcloudgame_int','valista_int','leadgenetics_int','manplus_int','Facebook Ads'
        ,'Facebook_onelink','carrot','afreecatvda_int','inmobi_int','push']

# 데이터 필터링
data_set = data_set.loc[data_set['media_source'].isin(source_list)]
data_set = data_set.loc[data_set['attributed_touch_type']=='click']
data_set = data_set.loc[(data_set['is_primary_attribution']!=False)]
data_set.loc[data_set['media_source'] == 'push', 'media_source'] = '_push'
data_set = data_set.sort_values(['appsflyer_id', 'install_time', 'media_source'])
data_set.loc[data_set['media_source'] == '_push', 'media_source'] = 'push'

# conversion_data
conversion_event = ['install', 're-engagement', 're-attribution']
conversion_data_columns = ['attributed_touch_time', 'media_source', 'install_time', 'appsflyer_id', 'event_name',
                           'channel', 'keywords', 'campaign', 'campaign_id',
                           'adset', 'adset_id', 'ad', 'ad_id', 'site_id', 'sub_site_id',
                           'sub_param_1', 'sub_param_2', 'sub_param_3', 'sub_param_4', 'sub_param_5',
                           'is_retargeting', 'is_primary_attribution']
conversion_data = data_set.loc[data_set['event_name'].isin(conversion_event), conversion_data_columns]
conversion_data = conversion_data.drop_duplicates(['appsflyer_id', 'install_time', 'media_source'])

conversion_data.index = range(len(conversion_data))

# diff_data
diff_data = conversion_data.groupby('appsflyer_id')['install_time'].diff()
diff_data = pd.DataFrame({'diff' : diff_data.values}, index = diff_data.index)
diff_data['diff'] = diff_data['diff'].apply(lambda x : x.days)

# push_data
push_data = pd.concat([conversion_data, diff_data], axis = 1)
push_data = push_data.loc[(push_data['media_source']=='push')&
                          (push_data['diff']<1), ['appsflyer_id', 'install_time', 'media_source']]

# before_data
before_data = conversion_data.loc[push_data.index - 1]
before_data.columns =['pre_' + col for col in before_data.columns]
before_data.index = (before_data.index + 1)

# appsflyer_push_data
appsflyer_push_data = pd.concat([push_data, before_data], axis= 1)
appsflyer_push_data = appsflyer_push_data.sort_values(['appsflyer_id', 'install_time'])
appsflyer_push_data.index = range(len(appsflyer_push_data))


# push_convert
convert_columns = ['channel', 'keywords', 'campaign', 'campaign_id',
                           'adset', 'adset_id', 'ad', 'ad_id', 'site_id', 'sub_site_id',
                           'sub_param_1', 'sub_param_2', 'sub_param_3', 'sub_param_4', 'sub_param_5',
                           'is_retargeting', 'is_primary_attribution', 'media_source']

push_convert = data_set.merge(appsflyer_push_data, on = ['appsflyer_id', 'install_time', 'media_source'], how = 'left')
for col in convert_columns :
    push_convert.loc[(push_convert['media_source'] == 'push') &
                     (push_convert['pre_media_source'].notnull()), col] = push_convert['pre_' + col]
push_convert = push_convert.loc[push_convert['event_time'].dt.month == 8]
# push_convert.to_csv(dr.download_dir + '/push_convert.csv', index=False, encoding = 'utf-8-sig')

# 1차 가공 완료 =================================================================================

# 2차 가공 (리포트 가공 절차) 시작 =================================================================
report_data = push_convert.loc[push_convert['event_name'].isin(ms.target_event)]
report_data = report_data.loc[report_data['media_source']!='push']
report_data['event_revenue'] = report_data['event_revenue'].apply(lambda x : float(str(x).replace('{}', '0')))

# google_channel_condition
google_drop_index = report_data.loc[(report_data['media_source']=='googleadwords_int') &
                                    ~(report_data['channel'].isin(['ACI_Search','ACE_Search','ACE_Youtube','ACE_Display','ACI_Youtube','ACI_Display']))].index
report_data = report_data.drop(index=google_drop_index)

# 페이스북 RE 제외
facebook_drop_index = report_data.loc[report_data['media_source'].isin(['Facebook Ads', 'Facebook_onelink']) &
                                      ((report_data['campaign'].str.contains('_RT_')) | (report_data['campaign'] == 'Madit_CNV_retargeting'))].index
report_data = report_data.drop(index=facebook_drop_index)

# attributed_touch_time 채우기
report_data.loc[report_data['attributed_touch_time'].isnull(), 'attributed_touch_time'] = report_data['install_time']

# CTET 계산
report_data['CTET'] = report_data['event_time'] - report_data['attributed_touch_time']
report_data['CTET'] = report_data['CTET'].apply(lambda x : x.total_seconds() / 86400)

for window in ms.attribution_window :
    report_data.loc[report_data['CTET']<=ms.attribution_window_dict.get(window), window] = True

report_data[ms.attribution_window] = report_data[ms.attribution_window].fillna(False)

report_data['click_date'] = report_data['attributed_touch_time'].dt.date
report_data['event_date'] = report_data['event_time'].dt.date
report_data['Cnt'] = 1


# 이벤트 집계

def event_pivoting(metric_group, raw_data, subset):
    pivot_index = ['date', 'campaign', 'adset', 'ad', 'channel', 'platform', 'is_retargeting']
    result_pivot_list = []

    for date in ['event_date', 'click_date']:
        if date == 'click_date':
            # 터치타임이 당월인 데이터만 남김
            raw_data = raw_data.loc[raw_data['attributed_touch_time'].dt.month == 8]
            raw_data.index = range(len(raw_data))

        raw_data['date'] = raw_data[date]
        raw_data[pivot_index] = raw_data[pivot_index].fillna('ㅁ')

        pivot_list = []
        for window in ms.attribution_window:
            window_data = raw_data.loc[raw_data[window] == True]

            if 'uni' in metric_group :
                window_data = window_data.sort_values(['event_time', 'install_time', 'attributed_touch_time'])
                window_data = window_data.drop_duplicates(subset)


            pivot_data = window_data.pivot_table(index=pivot_index,
                                                 columns='event_name',
                                                 values=['Cnt', 'event_revenue'],
                                                 aggfunc='sum')
            pivot_data.columns = ['/'.join(col) + '/' + window + '/' + date for col in pivot_data.columns]

            pivot_list.append(pivot_data)

        pivot_concat = pd.concat(pivot_list, axis=1)

        metric_list = ms.get_metric_list(metric_group, 'date_type', date)
        pivot_concat = pivot_concat[metric_list]

        result_pivot_list.append(pivot_concat)

    result_pivot_total = pd.concat(result_pivot_list, axis=1)
    result_pivot_total.columns = [ms.metric_group[metric_group][col] for col in result_pivot_total.columns]
    return result_pivot_total


# cn (Count)
cn_pivot_total = event_pivoting('cn', report_data, ['appsflyer_id', 'event_name'])

# uni (Unique)
uni_pivot_total = event_pivoting('uni', report_data, ['appsflyer_id', 'event_name'])
uni_pivot_total.sum()

# uni (Unique)
report_data_vlh = report_data.loc[report_data['event_name'].str.contains('Viewed LA Home')]
uni_vlh_pivot_total = event_pivoting('uni_vlh', report_data_vlh, ['appsflyer_id'])

# total events
total_event_pivot = pd.concat([cn_pivot_total, uni_pivot_total, uni_vlh_pivot_total], axis = 1)
total_event_pivot = total_event_pivot.fillna(0)
total_event_pivot = total_event_pivot.reset_index()

# 계산된 필드
total_event_pivot['total_install(uni) D0.T'] = total_event_pivot['install(uni) D0.T'] + \
                                               total_event_pivot['re-attribution(uni) D0.T'] + \
                                               total_event_pivot['re-engagement(uni) D0.T']
total_event_pivot['VLH+N(uni) D0.T'] = total_event_pivot['VLH(uni) D0.T'] + total_event_pivot['VLHN(uni) D0.T']
total_event_pivot.to_csv(dr.download_dir + '/finda_total_event_pivot.csv', index=False, encoding='utf-8-sig')