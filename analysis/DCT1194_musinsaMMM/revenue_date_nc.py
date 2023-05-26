import pandas as pd
from setting import directory as dr
import datetime

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/MMM/NC_RD/무신사_통합_캠페인 리포트_MMM_분석용.xlsx'
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/MMM/result_data'

rename = {
    '광고비_Fee포함' : 'S',
    '매체' : 'media',
    '일' : 'date',
    '카테고리매출' : 'revenue',
    '노출' : 'I',
    '링크 클릭' : 'C'}

media_rename = {
    'facebook' : 'Facebook',
    'twitter' : 'Twitter',
    'kakaovx' : 'KakaoVX',
    'kakaoVX' : 'KakaoVX',
    'tiktok': 'Tiktok',
    'kakao': 'Kakao'}

def NC_RAW():
    data =  pd.read_excel(raw_dir , sheet_name='RAW_NC',usecols= rename.keys())
    data['일'] =  pd.to_datetime(data['일'])
    data['일'] = data['일'].dt.date
    data['카테고리매출'] = data['카테고리매출'].fillna(0)
    data = data.rename(columns = rename)
    data['구분'] = '-'

    data.loc[data['구분'] == '-'  , 'media'] = data['media'].apply(
        lambda x: x.replace(x, media_rename[x]) if x in media_rename.keys() else x)

    kpi_data = data.groupby(['date'])[['revenue']].sum().reset_index()
    media_data = data.pivot_table(index='date', columns= 'media' , values=['I', 'S'], aggfunc='sum').reset_index().fillna(0)

    media_data.columns = [media + '_' + value for value, media in media_data.columns]
    media_data = media_data.rename(columns = {'_date' : 'date'})

    result_data = pd.merge(kpi_data , media_data , on ='date', how = 'left')
    result_data.to_csv(result_dir + '/nc_revenue.csv', index =False, encoding = 'utf-8-sig')

    return result_data

def promotion_data():

    promotion_data = pd.read_excel(raw_dir, sheet_name='프로모션일정',header= 2, usecols= ['계정','상세구분', '시작일', '종료일', '프로모션구분'])
    promotion_data = promotion_data.loc[promotion_data['계정'] =='NC']

    promotion_data['시작일'] = pd.to_datetime(promotion_data['시작일'])
    promotion_data['종료일'] = pd.to_datetime(promotion_data['종료일'])
    promotion_data['시작일'] = promotion_data['시작일'].dt.date
    promotion_data['종료일'] = promotion_data['종료일'].dt.date

    #프로모션 일정정리
    promotion_data.index = range(len(promotion_data))
    promotion_df = pd.DataFrame({'date': '-', 'pr' : '-','reg': '-', '상세구분': '-'}, index=[0])

    for i in range(len(promotion_data)):
        start_date = promotion_data['시작일'][i]
        last_date = promotion_data['종료일'][i]
        index = promotion_data['상세구분'][i]
        promotion = promotion_data['프로모션구분'][i]
        pr = 0
        reg = 0

        while start_date <= last_date:
            date = start_date.strftime("%Y-%m-%d")
            if promotion == '프로모션':
                pr = 1
            else :
                reg = 1
            append_df = pd.DataFrame(
                {'date': date, 'pr': pr, 'reg': reg, '상세구분': index}, index=[0])
            promotion_df = promotion_df.append(append_df, ignore_index=True)
            start_date += datetime.timedelta(days=1)

    promotion_df = promotion_df.loc[promotion_df['date'] != '-']
    promotion_df = promotion_df.groupby(['date'])['pr','reg'].sum().reset_index()
    promotion_df.loc[promotion_df['pr'] >= 1, 'pr'] = 1
    promotion_df.loc[promotion_df['reg'] >= 1, 'reg'] = 1

    return promotion_df

revenue = NC_RAW()
promotion = promotion_data()
promotion['date'] = pd.to_datetime(promotion['date'])
promotion['date'] = promotion['date'].dt.date

result_df = pd.merge(revenue,promotion,on='date',how='left')
result_df = result_df.fillna(0)

result_df.to_csv(result_dir + '/nc_reve_promo.csv', index =False, encoding = 'utf-8-sig')