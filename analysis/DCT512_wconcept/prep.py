from analysis.DCT512_wconcept import info
import pandas as pd
import os
from workers import read_data
import datetime
import re
from setting import directory as dr

file_dir = info.paid_dir
file_list = os.listdir(file_dir)

df = read_data.pyarrow_csv(dtypes = info.dtypes, directory = info.paid_dir, file_list = file_list)

def data_prep():

    df['order_id'] = df['event_value'].apply(lambda x:x.split('af_order_no')[-1].replace('"', '').replace('}', '').replace(']','').replace(':', '').replace('\\', '').split(',')[0] if x.find('af_order_no') != -1 else '-')

    df['attributed_touch_time'] = pd.to_datetime(df['attributed_touch_time'])
    df['install_time'] = pd.to_datetime(df['install_time'])
    df['event_time'] = pd.to_datetime(df['event_time'])

    df = df.loc[(df['install_time'] - df['attributed_touch_time']) < datetime.timedelta(days=1)]
    df = df.loc[(df['event_time'] - df['install_time']) < datetime.timedelta(days=1)]

    #일단 소재 내에 해당 프로모션 값이 있는지 체크 먼저하기 > 변환까지 해야함

    promotion_dict = {'TLblackfriday':'블랙프라이데이','TLwintershop':'윈터샵','MKTchaneldraw10':'샤넬드로우','timecoupon':'40% 타임쿠폰','PKnikedrawsep' : '나이키드로우1',
                      'Pkdraw':'나이키드로우2','MKTdoona':'배두나캠페인','doona':'배두나캠페인','bigsmile':'빅스마일데이','landersday':'랜더스데이','springshop':'스프링샵','happynewyear':'설연휴특가'}

    df['프로모션'] = '-'

    for i in promotion_dict.keys():
        df.loc[df['프로모션'] == '-', '프로모션'] = df['ad'].apply(lambda x: x.replace(x, promotion_dict[i]) if x.find(i) != -1 else '-')

    # url 내에 프로모션 있는지 확인 > 변환까지 해야함

    iscode_dict = {'issue/24694':'블랙프라이데이','issue/24709':'블랙프라이데이','issue/23605':'윈터샵','issue/23606':'윈터샵','issue/24467':'샤넬드로우','issue/24318':'40% 타임쿠폰','draw/106' : '나이키드로우1',
                      'draw/108':'나이키드로우2','issue/22940':'배두나캠페인','issue/20323':'빅스마일데이','issue/20324':'빅스마일데이','issue/19386':'랜더스데이','issue/19211':'스프링샵','issue/19301':'스프링샵','issue/18692':'설연휴특가'}

    for i in iscode_dict.keys():
        df.loc[df['프로모션'] == '-', '프로모션'] = df['original_url'].apply(lambda x: x.replace(x, iscode_dict[i]) if x.find(i) != -1 else '-')

    # 기간 필터링

    df = df.loc[df['프로모션'] != '-']

    start_dict = {'블랙프라이데이': '2022-11-14','윈터샵':'2022-10-31','샤넬드로우':'2022-10-27','40% 타임쿠폰':'2022-10-26','나이키드로우1':'2022-09-09','나이키드로우2':'2022-09-22','배두나캠페인':'2022-08-22','빅스마일데이':
                  '2022-05-12','랜더스데이':'2022-04-01','스프링샵':'2022-03-01','설연휴특가':'2022-01-24'}

    df['시작일자'] = '-'

    for i in start_dict.keys():
        df.loc[df['시작일자'] == '-' , '시작일자'] = df['프로모션'].apply(lambda x: x.replace(x, start_dict[i]) if x.find(i) != -1 else '-')


    end_dict = {'블랙프라이데이': '2022-11-28','윈터샵':'2022-11-15','샤넬드로우':'2022-11-01','40% 타임쿠폰':'2022-10-31','나이키드로우1':'2022-09-13','나이키드로우2':'2022-10-1','배두나캠페인':'2022-08-28','빅스마일데이':
                  '2022-05-28','랜더스데이':'2022-04-09','스프링샵':'2022-04-01','설연휴특가':'2022-02-08'}

    df['종료일자'] = '-'

    for i in end_dict.keys():
        df.loc[df['종료일자'] == '-' , '종료일자'] = df['프로모션'].apply(lambda x: x.replace(x, end_dict[i]) if x.find(i) != -1 else '-')

    df['시작일자'] = df['시작일자'].apply(pd.to_datetime)
    df['종료일자'] = df['종료일자'].apply(pd.to_datetime)

    df = df.loc[(df['시작일자'] <= df['install_time']) & (df['install_time'] <= df['종료일자'])]

    order_id = df[['event_time','appsflyer_id','order_id']]
    order_id = order_id.loc[order_id['order_id'] != '-']

    order_id.to_csv(info.raw_dir + '/첫구매_orderid_전달용.csv', index=False, encoding='utf-8-sig')

    cust_id = df.loc[df['order_id'] == '-']
    cust_id['cust_id'] = cust_id['event_value'].apply(lambda x: x.split('af_cust_no')[-1].replace('"', '').replace('}', '').replace(']', '').replace(':', '').replace('\\', '').split(',')[0] if x.find('af_cust_no') != -1 else '-')
    cust_id = cust_id.loc[cust_id['cust_id'] != '-']

    cust_id = cust_id[['event_time','appsflyer_id','cust_id']]

    cust_id.to_csv(info.raw_dir + '/첫구매_custid_전달용.csv', index=False, encoding='utf-8-sig')

    return df

