import pandas as pd
from setting import directory as dr

class rename:
    conv_rename_dic = {'date': 'date',
                  'seg': 'seg',
                  'media_source' : 'media',
                  'campaign': 'campaign',
                  '합계 : 앱 설치 수': 'install',
                  '합계 : 앱 첫 구매 수': 'first_purchase'}

    media_rename_dic = {'날짜': 'date',
                        '매체' : 'media',
                        '캠페인': 'campaign',
                        '노출 ': 'I',
                        '클릭 ': 'C',
                        '광고비 (vat+)': 'S'}

# 결과 파일 경로
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1427'
raw_dir = result_dir + '/RD'

def data_prep():

    conv_data = pd.read_csv(raw_dir + '/conversion.csv', usecols= rename.conv_rename_dic.keys())
    conv_data = conv_data.rename(columns= rename.conv_rename_dic)
    conv_data = conv_data.loc[conv_data['date'] != '총합계']
    conv_data['campaign'] = conv_data['campaign'].fillna('')
    conv_data['media_campaign'] = conv_data['media'] + '_' + conv_data['campaign']
    conv_data['date'] = pd.to_datetime(conv_data['date'])
    conv_data['install_campaign'] = 0
    conv_data.loc[conv_data['media_campaign'] == '페이스북_', 'media_campaign'] = '페이스북_앱설치'
    conv_data.loc[conv_data['media_campaign'].isin(['카카오_앱설치','구글_AC','페이스북_앱설치']),'install_campaign'] = conv_data['install']

    kpi_data = conv_data.groupby(['date'])['install_campaign','first_purchase'].sum().reset_index()

    media_data = pd.read_excel(raw_dir + '/media.xlsx',sheet_name=1)
    media_data = media_data.rename(columns = rename.media_rename_dic)
    media_data['date'] = pd.to_datetime(media_data['date'])
    media_data['campaign'] = media_data['campaign'].fillna('')
    media_data['media_campaign'] = media_data['media'] + '_' + media_data['campaign']

    media_data['sum'] = media_data['I'] + media_data['C'] + media_data['S']
    media_data = media_data.loc[media_data['sum'] > 0]

    # 인스톨 캠페인 합치기
    media_data.loc[media_data['media_campaign'].isin(['페이스북_앱설치','카카오_앱설치','구글_AC' ]),'media_campaign'] = '앱설치캠페인'

    media_data = media_data.pivot_table(index='date', columns=['media_campaign'], values=['I', 'S'], aggfunc='sum').reset_index().fillna(0)
    media_data.columns = [media + '_' + value for value, media in media_data.columns]
    media_data = media_data.rename(columns={'_date': 'date'})

    merge_data = pd.merge(kpi_data,media_data, on = 'date', how = 'left').fillna(0)

    promotion_data = pd.read_excel(raw_dir + '/promotion.xlsx',sheet_name=0)
    promotion_data = promotion_data.rename(columns = {'날짜':'date','프로모션 진행 여부':'promotion'})
    merge_data = pd.merge(merge_data, promotion_data, on='date', how='left').fillna(0)

    return merge_data

result_data = data_prep()
result_data.to_csv(result_dir + '/result_data_totalinstall.csv', index = False, encoding = 'utf-8-sig')
