import datetime
import pandas as pd

import analysis.DCT1427_29CM_firstpurchase_analysis.PSM_data_prep as dprep
from workers.func import FunnelDataGenerator

# EDA Depth1. 전환율 비교
def first_purchase_check():

    data = dprep.data_prep()
    data = data.drop_duplicates()
    data.index = range(len(data))

    psm_data = pd.read_csv(dprep.directory.result_dir + '/psm_data.csv')

    psm_group = psm_data['appsflyer_id'].to_list()
    treat_group = psm_data.loc[psm_data['treat'] == 1]['appsflyer_id'].to_list()
    control_group = psm_data.loc[psm_data['treat'] == 0]['appsflyer_id'].to_list()

    data = data.loc[data['appsflyer_id'].isin(psm_group)]
    data['treat'] = '-'

    data.loc[data['appsflyer_id'].isin(treat_group),'treat'] = True
    data.loc[data['appsflyer_id'].isin(control_group),'treat'] = False

    data.loc[data['campaign'] == '', 'campaign'] = 'restricted'

    return data

def install_ITET() :

    data = first_purchase_check()
    install_time = data.loc[data['event_name'].isin(['install','re-engagement','re-attribution'])]
    install_time = install_time.sort_values('install_time')
    install_time = install_time.drop_duplicates('appsflyer_id',keep='first')
    install_time['install_campaign_time'] = install_time['install_time']
    install_time['install_campaign'] = install_time['campaign']
    install_time = install_time[['appsflyer_id', 'install_campaign_time','install_campaign']].drop_duplicates(subset='appsflyer_id')

    raw_data = pd.merge(data,install_time,on='appsflyer_id',how='left').fillna('-')

    new_purchaser = raw_data.loc[raw_data['event_name'] == 'new_purchaser']
    new_purchaser = new_purchaser.loc[new_purchaser['install_campaign_time'] < new_purchaser['event_time']]

    new_purchaser['newpurchase_time'] = new_purchaser['event_time']
    new_purchaser['newpurchase_campaign'] = new_purchaser['campaign']
    new_purchaser = new_purchaser[['appsflyer_id', 'newpurchase_time', 'newpurchase_campaign']].drop_duplicates(subset='appsflyer_id')

    raw_data = pd.merge(raw_data, new_purchaser, on='appsflyer_id', how='left').fillna('-')

    raw_data['install_campaign_time'] = pd.to_datetime(raw_data['install_campaign_time'])
    raw_data['event_time'] = pd.to_datetime(raw_data['event_time'])

    raw_data['ITET'] = raw_data['event_time'] - raw_data['install_campaign_time']

    raw_data['ITET'] = raw_data['ITET'].apply(lambda x: str(x).split('days')[0])
    raw_data['ITET'] = raw_data['ITET'].astype(int)

    raw_data = raw_data.loc[raw_data['ITET'] >= 0]

    raw_data['is_paid'] = False
    raw_data.loc[raw_data['media_source'] =='rtbhouse_int://','media_source'] = 'rtbhouse_int'
    raw_data.loc[raw_data['media_source'].isin(['29cm_mowebtoapp','criteonew_int','Facebook Ads','fbig','googleadwords_int','kakao_int','naver_gfa','naverbsa','naversa','restricted','rtbhouse_int']),'is_paid'] = True

    raw_data = raw_data.drop_duplicates(['appsflyer_id','event_time','event_name'])

    return raw_data


def campaign_LTV():

    raw_data = install_ITET()

    # 인스톨 캠페인과 첫구매 캠페인이 같은 유저 선별
    user = raw_data.loc[(raw_data['install_campaign'] == raw_data['newpurchase_campaign'])]
    user = user['appsflyer_id'].drop_duplicates().tolist()

    raw_data = raw_data.loc[raw_data['appsflyer_id'].isin(user)]

    purchase_data = raw_data.loc[raw_data['event_name'].isin(['af_purchase','new_purchaser'])]

    purchase = purchase_data.pivot_table(index='appsflyer_id',columns='event_name', values='event_revenue',aggfunc='count').reset_index()
    purchase = purchase.rename(columns = {'af_purchase':'purchase','new_purchaser':'new_purchase'}).fillna(0)

    LTV = purchase_data.pivot_table(index='appsflyer_id',columns='event_name', values='event_revenue',aggfunc='sum').reset_index()
    LTV = LTV.rename(columns={'af_purchase': 'LTV','new_purchaser':'new_LTV'}).fillna(0)

    index = purchase_data[['appsflyer_id','newpurchase_campaign','install_campaign','treat']].drop_duplicates('appsflyer_id')

    data = pd.merge(index,purchase ,on= 'appsflyer_id',how='left')
    data = pd.merge(data,LTV,on = 'appsflyer_id',how='left')

    # 인스톨 캠페인과 아닌 캠페인 비교

    data['columns'] ='-'
    treat_data = data.pivot_table(index='treat',columns='columns',values=['LTV','purchase'],aggfunc='sum').reset_index()
    user = data.pivot_table(index='treat', columns='columns', values='appsflyer_id',aggfunc='count',margins=True).reset_index()
    treat_data.columns =['treat','LTV','purchase']

    treat = data.loc[data['treat'] == True]

    install_campaign = treat.pivot_table(index='install_campaign',columns='treat',values=['LTV','purchase'],aggfunc='sum',margins=True).reset_index()
    install_campaign_user = treat.pivot_table(index='install_campaign',columns='treat',values='appsflyer_id',aggfunc='count',margins=True).reset_index()

    report_data = pd.read_excel(dprep.directory.result_dir + '/report.xlsx', sheet_name=0)
    report_data = report_data[['날짜', 'SPEND_AGENCY (vat +)', '캠페인', '매체']]

    report_data['날짜'] = pd.to_datetime(report_data['날짜'])
    report_data = report_data.loc[(report_data['날짜'] >= datetime.datetime(year=2023, month=4, day=1)) & (
                report_data['날짜'] <= datetime.datetime(year=2023, month=7, day=30))]

    report_data['treat'] = False
    report_data.loc[report_data['캠페인'].isin(dprep.columns.report_rename.keys()), 'treat'] = True
    report_data['캠페인'] = report_data['캠페인'].apply(lambda x: x.replace(x, dprep.columns.report_rename[x]) if x in dprep.columns.report_rename.keys() else x)

    report_data = report_data.loc[report_data['treat'] == True]
    report_data = report_data.groupby(['날짜','캠페인'])['SPEND_AGENCY (vat +)'].sum().reset_index()
    report_data = report_data.rename(columns={'날짜':'event_time','캠페인':'campaign','SPEND_AGENCY (vat +)':'spend'})
    report_data['event_time'] = report_data['event_time'].dt.date

    # acq 정제

    raw_data = install_ITET()

    # 인스톨 캠페인과 첫구매 캠페인이 같은 유저 선별
    user = raw_data.loc[(raw_data['install_campaign'] == raw_data['newpurchase_campaign'])]
    user = user['appsflyer_id'].drop_duplicates().tolist()

    raw_data = raw_data.loc[raw_data['appsflyer_id'].isin(user)]
    raw_data = raw_data.loc[raw_data['event_name'] =='new_purchaser']
    raw_data = raw_data.loc[raw_data['event_time'] == raw_data['newpurchase_time']]

    raw_data['event_time'] = raw_data['event_time'].dt.date
    raw_data = raw_data.loc[raw_data['treat'] ==True]

    acq_user = data[['appsflyer_id','LTV']]
    acq_user = pd.merge(raw_data,acq_user, on = 'appsflyer_id',how='left')

    acq_user = acq_user.pivot_table(index=['event_time','campaign'],columns='treat',values='LTV',aggfunc='sum').reset_index().rename(columns={True:'acq'})

    acq_data = pd.merge(report_data,acq_user, on= ['event_time','campaign'],how='left').fillna(0)

    acq_data.to_csv(dprep.directory.result_dir +'/acq_data.csv',index=False,encoding='utf-8-sig')

    return acq_data


# 다시....분석

def labeling():

    data = first_purchase_check()

    data['is_paid'] = False
    data.loc[data['media_source'] =='rtbhouse_int://','media_source'] = 'rtbhouse_int'
    data.loc[data['media_source'].isin(['29cm_mowebtoapp','criteonew_int','Facebook Ads','fbig','googleadwords_int','kakao_int','naver_gfa','naverbsa','naversa','restricted','rtbhouse_int']),'is_paid'] = True

    # 최초 유입 매체 라벨링
    install_time = data.loc[data['event_name'].isin(['install', 're-engagement', 're-attribution'])]
    install_time = install_time.sort_values('install_time')
    install_time = install_time.drop_duplicates('appsflyer_id', keep='first')
    install_time['install_campaign_time'] = install_time['install_time']
    install_time['install_campaign'] = install_time['campaign']
    install_time['install_media'] = install_time['media_source']
    install_time['install_is_paid'] = False
    install_time.loc[install_time['media_source'].isin(['29cm_mowebtoapp','criteonew_int','Facebook Ads','fbig','googleadwords_int','kakao_int','naver_gfa','naverbsa','naversa','restricted','rtbhouse_int']),'install_is_paid'] = True
    install_time = install_time[['appsflyer_id', 'install_campaign_time', 'install_campaign','install_media','install_is_paid','treat']]

    raw_data = pd.merge(data, install_time, on=['appsflyer_id','treat'], how='left').fillna('-')
    raw_data['cnt'] = 1

    # 구매 라벨링
    purchase_data = raw_data.loc[raw_data['event_name'] == 'af_purchase']
    purchase_data = purchase_data.pivot_table(index='appsflyer_id',columns='event_name',values=['event_revenue','cnt'],aggfunc='sum').reset_index()
    purchase_data.columns = ['appsflyer_id','total_purchase','total_revenue']

    # 첫구매 라벨링
    new_purchase_data = raw_data.loc[raw_data['event_name'] == 'new_purchaser']
    new_purchase_data = new_purchase_data.drop_duplicates(subset = 'appsflyer_id',keep= 'first')
    new_purchase_data['newpurchase_time'] = new_purchase_data['event_time']
    new_purchase_data['newpurchase_time'] = pd.to_datetime(new_purchase_data['newpurchase_time'])

    new_purchase_data = new_purchase_data.loc[new_purchase_data['newpurchase_time'] > new_purchase_data['install_campaign_time']]

    new_purchase_data['newpurchase_campaign'] = new_purchase_data['campaign']
    new_purchase_data['newpurchase_media'] = new_purchase_data['media_source']
    new_purchase_data['newpurchase_is_paid'] = False
    new_purchase_data['newpurchase_revenue'] = new_purchase_data['event_revenue']

    new_purchase_data.loc[new_purchase_data['media_source'].isin(['29cm_mowebtoapp', 'criteonew_int', 'Facebook Ads', 'fbig', 'googleadwords_int', 'kakao_int', 'naver_gfa','naverbsa', 'naversa', 'restricted', 'rtbhouse_int']), 'newpurchase_is_paid'] = True
    new_purchase_data = new_purchase_data[['appsflyer_id', 'newpurchase_time', 'newpurchase_campaign', 'newpurchase_media', 'newpurchase_is_paid','newpurchase_revenue']]

    #데이터 머징
    data = pd.merge(install_time,purchase_data,on='appsflyer_id',how='left').fillna(0)
    data = pd.merge(data,new_purchase_data,on='appsflyer_id',how='left').fillna('-')
    data['new_purchaser'] = 1
    data.loc[data['newpurchase_is_paid'] =='-','new_purchaser'] = 0
    data.loc[data['newpurchase_revenue'] == '-', 'newpurchase_revenue'] = 0
    data['cnt'] = 1

    #ITET 구하기
    data['ITET'] = 0
    new = data.loc[data['new_purchaser'] == 1]
    nonew = data.loc[data['new_purchaser'] == 0]

    new['install_campaign_time'] = pd.to_datetime(new['install_campaign_time'])
    new['newpurchase_time'] = pd.to_datetime(new['newpurchase_time'])
    new.loc[new['new_purchaser']== 1,'ITET'] = new['newpurchase_time'] - new['install_campaign_time']

    new['ITET'] = new['ITET'].astype('timedelta64[D]').astype('int')
    data = pd.concat([new,nonew])

    data.to_csv(dprep.directory.result_dir +'/29cm_data.csv',index=False, encoding= 'utf-8-sig')

    return data


def acq_data_perp():

    user = data.loc[data['new_purchaser'] == 1]
    user = user['appsflyer_id'].drop_duplicates().tolist()

    user_data = raw_data.loc[raw_data['appsflyer_id'].isin(user)]
    user_revenue = user_data.pivot_table(index='appsflyer_id',columns='event_name',values='event_revenue',aggfunc='sum').reset_index()
    user_revenue = user_revenue[['appsflyer_id','af_purchase']].rename(columns={'af_purchase':'revenue'})

    user_install = user_data[['appsflyer_id','install_campaign_time','install_campaign','install_media','install_is_paid','treat']]
    user_install = user_install.loc[user_install['treat'] == True]
    user_install = user_install.drop_duplicates('appsflyer_id')

    user_install['install_campaign_time'] = pd.to_datetime(user_install['install_campaign_time'])
    user_install['install_campaign_time'] = user_install['install_campaign_time'].dt.date

    user = pd.merge(user_install,user_revenue, on= 'appsflyer_id',how= 'left')

    user_cnt = user_install.pivot_table(index=['install_campaign_time','install_campaign','install_media'],columns ='treat',values ='appsflyer_id',aggfunc='count').reset_index()
    user_cnt = user_cnt.rename(columns={True:'acq_user'})

    user = user.pivot_table(index=['install_campaign_time','install_campaign','install_media'],columns ='treat',values ='revenue',aggfunc='sum').reset_index()
    user = user.rename(columns={True: 'revenue'})

    user = pd.merge(user,user_cnt,on=['install_campaign_time','install_campaign','install_media'],how='left')

    report_data = pd.read_excel(dprep.directory.result_dir + '/report.xlsx', sheet_name=0)
    report_data = report_data[['날짜', 'SPEND_AGENCY (vat +)', '캠페인', '매체']]

    report_data['날짜'] = pd.to_datetime(report_data['날짜'])
    report_data = report_data.loc[(report_data['날짜'] >= datetime.datetime(year=2023, month=4, day=1)) & (
            report_data['날짜'] <= datetime.datetime(year=2023, month=7, day=30))]

    report_data['treat'] = False
    report_data.loc[report_data['캠페인'].isin(dprep.columns.report_rename.keys()), 'treat'] = True
    report_data['캠페인'] = report_data['캠페인'].apply(
        lambda x: x.replace(x, dprep.columns.report_rename[x]) if x in dprep.columns.report_rename.keys() else x)

    report_data = report_data.loc[report_data['treat'] == True]
    report_data = report_data.groupby(['날짜', '캠페인'])['SPEND_AGENCY (vat +)'].sum().reset_index()
    report_data = report_data.rename(columns={'날짜': 'event_time', '캠페인': 'install_campaign', 'SPEND_AGENCY (vat +)': 'spend'})
    report_data['event_time'] = report_data['event_time'].dt.date
    report_data = report_data.rename(columns={'event_time': 'install_campaign_time'})

    report_data = report_data.groupby(['install_campaign_time', 'install_campaign'])['spend'].sum().reset_index()

    acq_data = pd.merge(report_data,user, on = ['install_campaign_time', 'install_campaign'],how='left').fillna(0)


    acq_data.to_csv(dprep.directory.result_dir +'/acq_data.csv',index=False, encoding= 'utf-8-sig')

    return acq_data