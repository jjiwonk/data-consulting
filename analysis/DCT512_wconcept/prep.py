from analysis.DCT512_wconcept import info
import pandas as pd
import os
from workers import read_data
import datetime
import setting.directory as dr
import json

def data_prep():
    file_dir = info.paid_dir
    file_list = os.listdir(file_dir)

    df = read_data.pyarrow_csv(dtypes=info.dtypes, directory=info.paid_dir, file_list=file_list)
    df = df.sort_values(['event_time', 'appsflyer_id', 'event_revenue'])


    df = df.drop_duplicates(['event_time','appsflyer_id','event_revenue'],keep='first')

    df['order_id'] = df['event_value'].apply(lambda x:x.split('af_order_no')[-1].replace('"', '').replace('}', '').replace(']','').replace(':', '').replace('\\', '').split(',')[0] if x.find('af_order_no') != -1 else '-')

    df['attributed_touch_time'] = pd.to_datetime(df['attributed_touch_time'])
    df['install_time'] = pd.to_datetime(df['install_time'])
    df['event_time'] = pd.to_datetime(df['event_time'])

    df = df.loc[(df['install_time'] - df['attributed_touch_time']) < datetime.timedelta(days=1)]
    df = df.loc[(df['event_time'] - df['install_time']) < datetime.timedelta(days=1)]

    # 일단 소재 내에 해당 프로모션 값이 있는지 체크 먼저하기 > 변환까지 해야함

    promotion_dict = {'TLblackfriday':'블랙프라이데이','TLwintershop':'윈터샵','MKTchaneldraw10':'샤넬드로우','timecoupon':'40% 타임쿠폰','PKdraw' : '나이키드로우1',
                      'PKnikedrawsep':'나이키드로우2','MKTdoona':'배두나캠페인','doona':'배두나캠페인','landersday':'랜더스데이','springshop':'스프링샵','happynewyear':'설연휴특가'}

    df['프로모션'] = '-'

    for i in promotion_dict.keys():
        df.loc[df['프로모션'] == '-', '프로모션'] = df['ad'].apply(lambda x: x.replace(x, promotion_dict[i]) if x.find(i) != -1 else '-')

    # url 내에 프로모션 있는지 확인 > 변환까지 해야함

    iscode_dict = {'issue/24694':'블랙프라이데이','issue/24709':'블랙프라이데이','issue/23605':'윈터샵','issue/23606':'윈터샵','issue/24467':'샤넬드로우','issue/24318':'40% 타임쿠폰','draw/106' : '나이키드로우1',
                      'draw/108':'나이키드로우2','issue/22940':'배두나캠페인','issue/19386':'랜더스데이','issue/19211':'스프링샵','issue/19301':'스프링샵','issue/18692':'설연휴특가'}

    for i in iscode_dict.keys():
        df.loc[df['프로모션'] == '-', '프로모션'] = df['original_url'].apply(lambda x: x.replace(x, iscode_dict[i]) if x.find(i) != -1 else '-')

    # 기간 필터링

    df = df.loc[df['프로모션'] != '-']

    start_dict = {'블랙프라이데이': '2022-11-14','윈터샵':'2022-10-31','샤넬드로우':'2022-10-27','40% 타임쿠폰':'2022-10-26','나이키드로우1': '2022-09-09' ,'나이키드로우2':'2022-09-22','배두나캠페인':'2022-08-22','랜더스데이':'2022-04-01','스프링샵':'2022-03-01','설연휴특가':'2022-01-24'}

    df['시작일자'] = '-'

    for i in start_dict.keys():
        df.loc[df['시작일자'] == '-' , '시작일자'] = df['프로모션'].apply(lambda x: x.replace(x, start_dict[i]) if x.find(i) != -1 else '-')

    end_dict = {'블랙프라이데이': '2022-11-28','윈터샵':'2022-11-15','샤넬드로우':'2022-11-01','40% 타임쿠폰':'2022-10-31','나이키드로우1':'2022-09-13','나이키드로우2':'2022-10-1','배두나캠페인':'2022-08-28','랜더스데이':'2022-04-09','스프링샵':'2022-04-01','설연휴특가':'2022-02-08'}

    df['종료일자'] = '-'

    for i in end_dict.keys():
        df.loc[df['종료일자'] == '-' , '종료일자'] = df['프로모션'].apply(lambda x: x.replace(x, end_dict[i]) if x.find(i) != -1 else '-')

    df['시작일자'] = df['시작일자'].apply(pd.to_datetime)
    df['종료일자'] = df['종료일자'].apply(pd.to_datetime)

    df = df.loc[(df['시작일자'] <= df['install_time']) & (df['install_time'] <= df['종료일자'])]

    # 첫구매 매핑
    first_user = pd.read_csv(info.raw_dir + '/첫구매유저.csv')
    first_user['첫구매'] = True
    first_user = first_user.drop_duplicates(['appsflyer_id'],keep = 'first')

    df = pd.merge(df,first_user,on ='appsflyer_id', how = 'left' ).fillna(False)
    df = df.drop_duplicates(['appsflyer_id'], keep='first')

    df = df.loc[df['첫구매'] == True]
    df['첫구매일자'] = df['event_time']

    return df

def acq_user_ARPU():

    file_dir = info.paid_dir
    file_list = os.listdir(file_dir)
    paid_df = read_data.pyarrow_csv(dtypes=info.dtypes, directory=info.paid_dir, file_list=file_list)

    file_dir = info.organic_dir
    file_list = os.listdir(file_dir)
    organic_df = read_data.pyarrow_csv(dtypes=info.organic_dtypes, directory=info.organic_dir, file_list=file_list)
    df = pd.concat([paid_df,organic_df]).fillna('-').sort_values('event_time')
    df = df.drop_duplicates(['event_time', 'appsflyer_id', 'event_revenue'], keep='first')

    df = df[['event_time','event_revenue','event_value','appsflyer_id']]
    df = df.loc[df['event_revenue'] != '-']

    promo_df = data_prep()
    promo_df = promo_df[['media_source','campaign','appsflyer_id','프로모션', '첫구매', '첫구매일자']].sort_values('첫구매일자')
    promo_df = promo_df.loc[promo_df['media_source'].isin(['KAKAOBIZBOARD', 'NAVERGFA', 'facebook', 'NAVERSPECIALDA','BRANDINGDA', 'Twitter', 'appier_int','adisonofferwall_int'])]

    promo_df.loc[promo_df['프로모션'] == '나이키드로우2','프로모션'] = '나이키드로우'
    promo_df.loc[promo_df['프로모션'] == '나이키드로우1', '프로모션'] = '나이키드로우'

    df = pd.merge(df,promo_df,on ='appsflyer_id', how = 'left' ).fillna('-')
    #
    acq_df = df.loc[df['프로모션'] != '-']
    acq_df['첫구매일자'] = pd.to_datetime(acq_df['첫구매일자'])
    acq_df = acq_df.loc[acq_df['첫구매일자'] <=  acq_df['event_time']]

    acq_df['구매종료일자'] = acq_df['첫구매일자'] + datetime.timedelta(days=60)
    acq_df_arpu = acq_df.loc[acq_df['event_time'] <= acq_df['구매종료일자']]

    acq_df['재구매시작일자'] = acq_df['첫구매일자'] + datetime.timedelta(days=1)
    acq_df['재구매종료일자'] = acq_df['재구매시작일자'] + datetime.timedelta(days=60)

    re_acq_df = acq_df.loc[(acq_df['재구매시작일자'] <= acq_df['event_time'])&(acq_df['event_time'] <= acq_df['재구매종료일자'])]
    re_acq_df['재구매'] = 1
    re_acq_df = re_acq_df[['appsflyer_id','재구매']].drop_duplicates('appsflyer_id',keep= 'first')

    total_revenue = acq_df_arpu.pivot_table(index = 'appsflyer_id', values = 'event_revenue', aggfunc = 'sum').reset_index()
    total_revenue = total_revenue.rename(columns = {'event_revenue' : 'total_revenue'})

    LTV = df.pivot_table(index = 'appsflyer_id', values = 'event_revenue', aggfunc = 'sum').reset_index()
    LTV = LTV.rename(columns = {'event_revenue':'LTV'})

    promo_df =  pd.merge(promo_df,total_revenue,on ='appsflyer_id', how = 'left' ).fillna('-')
    promo_df = pd.merge(promo_df, re_acq_df, on='appsflyer_id', how='left').fillna(0)
    promo_df = pd.merge(promo_df, LTV, on='appsflyer_id', how='left').fillna(0)
    promo_df['cnt'] = 1

    return promo_df

def pivoting(depth):

    promo_df = acq_user_ARPU()
    ARPU = promo_df.pivot_table(index= depth, values='total_revenue', aggfunc='mean',margins=True).reset_index().rename(columns = {'total_revenue':'ARPU'})
    retention = promo_df.pivot_table(index= depth, values=['재구매','cnt'], aggfunc='sum',margins=True).reset_index().rename(columns = {'cnt':'획득유저'})
    LTV = promo_df.pivot_table(index= depth, values='LTV', aggfunc='sum',margins=True).reset_index()

    piv_df = pd.merge(ARPU, retention, on= depth, how='left').fillna(0)
    piv_df = pd.merge(piv_df, LTV, on= depth, how='left').fillna(0)

    piv_df['Retention Rate'] = piv_df['재구매'] / piv_df['획득유저']
    piv_df['LTV(est)'] = piv_df['ARPU']/(1-piv_df['Retention Rate'])
    piv_df['LTV'] = piv_df['LTV'] / piv_df['획득유저']

    cost_df = pd.read_csv(info.raw_dir+'/프로모션비용.csv')
    cost_df = cost_df.groupby(depth)['cost'].sum().reset_index()
    piv_df = pd.merge(piv_df, cost_df, on= depth, how='left').fillna(0)

    piv_df['CAC'] = piv_df['cost'] / piv_df['획득유저']
    piv_df['LTV/CAC'] =  piv_df['LTV(est)'] / piv_df['CAC']

    return piv_df

df = pivoting(['구분']).fillna(0)
df.to_csv(info.raw_dir +'/result/promotion_ltv.csv',index = False, encoding = 'utf-8-sig')