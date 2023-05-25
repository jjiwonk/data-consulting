import pandas as pd
from setting import directory as dr

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/MMM/RD'
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/MMM/result_data'
rename = {
    '일' : 'date',
    '(MT_BZ)7d 매출' : 'paid_revenue',
    '노출' : 'I',
    '링크 클릭' : 'C',
    '광고비_Fee포함' : 'S',
    '매체' : 'media',
    '캠페인명' : 'category',
    'KPI' : 'campaign_type'}

def data_prep():
    data = pd.read_excel(raw_dir + '/무신사스탠다드_캠페인 리포트_MMM_분석용.xlsx', sheet_name='Sheet1',usecols= rename.keys())
    data = data.rename(columns=rename)

    data['date'] = pd.to_datetime(data['date'])
    data['date'] = data['date'].dt.date

    data.loc[data['category'] == '상시세일즈' , 'category'] = 'sales'
    data.loc[data['category'] == '여성유입', 'category'] = 'female'
    data['media'].unique()
    # 미디어 정제
    brand_click = data.loc[data['media'] == 'naver_brandsearch']
    brand_click = brand_click.pivot_table(index='date', columns=['media'], values=['C'],
                                  aggfunc='sum').reset_index().fillna(0)
    brand_click.columns  = ['date','naverbs_click']
    data = data.loc[data['media'].isin(['Facebook','KAKAO', 'Criteo'])]
    revenue_data = data.groupby(['date'])[['paid_revenue']].sum().reset_index()

    media_data = data.pivot_table(index='date', columns= ['media','category'], values=['I', 'S'], aggfunc='sum').reset_index().fillna(0)
    media_data.columns = [media + '_' + category + '_' + value for value, media , category in media_data.columns]
    media_data = media_data.rename(columns = {'__date' : 'date'})

    result_data = pd.merge(revenue_data , media_data , on ='date', how = 'left')
    result_data = pd.merge(result_data, brand_click, on='date', how='left')
    result_data.to_csv(result_dir + '/MT_MMM_RD.csv', index =False, encoding = 'utf-8-sig')

    return result_data