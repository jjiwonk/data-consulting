import setting.directory as dr
import setting.report_date as rdate
from setting.info import tableau_info
from spreadsheet import spreadsheet

import pandas as pd

class document():
    doc = spreadsheet.spread_document_read(
        'https://docs.google.com/spreadsheets/d/1efQrJpglhz0K9wKkl2QZ5VLrMu_hIUUnPG_uTSeI1Hk/edit#gid=2008544363')
    # 캠페인 정보 관리 시트 불러오기
    campaign_doc = spreadsheet.spread_document_read(
        'https://docs.google.com/spreadsheets/d/1w23AdEYLf4OJDlTElf2vwbVPttctSx-2nB51ZHQDEJQ/edit#gid=0')

def tableau_custom_sheet(doc):
    # 태블로 대시보드 컬럼 설정 불러오기
    sheet_data = spreadsheet.spread_sheet(doc, tableau_info.account_name)
    return sheet_data

def get_column_dict(sheet_data):
    sheet_data_column = sheet_data.loc[sheet_data['원본 리포트 기준']!= '입력 금지', ['원본 리포트 기준', '태블로 RD']]
    sheet_data_column = sheet_data_column.loc[sheet_data_column['원본 리포트 기준']!='']
    column_dict = dict(zip(sheet_data_column['원본 리포트 기준'], sheet_data_column['태블로 RD']))
    return column_dict

def client_rd_read(column_dict, encoding = 'utf-8-sig'):
    use_cols = column_dict.keys()
    ## 광고주 리포트 데이터 가져오기
    raw_data = pd.read_csv(tableau_info.raw_dir + f'/{tableau_info.account_name}/{tableau_info.result_name}_{rdate.yearmonth}.csv',
                           usecols=use_cols, encoding=encoding)

    raw_data = raw_data.rename(columns=column_dict)
    raw_data = raw_data.loc[pd.to_datetime(raw_data['날짜']).dt.month == rdate.day_1.month]

    # string to numeric
    value_columns = ['도달', '노출', '클릭', '비용', '구매', '매출', '첫구매', '첫구매 매출', '설치', '재설치', '재실행', '가입']
    columns = raw_data.columns

    for col in value_columns:
        if col in columns:
            raw_data[col] = raw_data[col].fillna(0)
            if raw_data[col].dtypes == 'object':
                raw_data[col] = raw_data[col].str.strip()
                raw_data[col] = raw_data[col].str.replace('-', '0')
                raw_data[col] = raw_data[col].str.replace(',', '')
                raw_data[col] = pd.to_numeric(raw_data[col])

    return raw_data

def campaign_info_sheet(campaign_doc):

    campaign_sheet = spreadsheet.spread_sheet(campaign_doc, '캠페인 정보')
    campaign_sheet = campaign_sheet.loc[campaign_sheet['광고주'] == tableau_info.account_name]
    campaign_sheet = campaign_sheet.loc[campaign_sheet['매체'].isin(['페이스북', '카카오', '구글'])]
    campaign_sheet = campaign_sheet[
        ['매체', '광고 상품', '캠페인', '광고그룹', '최적화 엔진', '캠페인 구분', '오디언스', 'OS', 'KPI', '캠페인 라벨']]
    campaign_sheet = campaign_sheet.drop_duplicates(['매체', '캠페인', '광고그룹'], keep='last')
    return campaign_sheet

    # 컬럼명 기준 딕셔너리 생성

def data_exception(raw_data, doc):
    # 매체별 필터 기준
    facebook_list = list(sheet_data.loc[sheet_data['페이스북 필터'].str.len() > 0, '페이스북 필터'])
    kakao_list = list(sheet_data.loc[sheet_data['카카오 필터'].str.len() > 0, '카카오 필터'])
    google_list = list(sheet_data.loc[sheet_data['구글 필터'].str.len() > 0, '구글 필터'])

    if len(facebook_list) != 0 :
        raw_data.loc[raw_data['매체'].isin(facebook_list), '매체'] = '페이스북'
    if len(kakao_list) != 0 :
        raw_data.loc[raw_data['매체'].isin(kakao_list), '매체'] = '카카오'
    if len(google_list) != 0:
        raw_data.loc[raw_data['매체'].isin(google_list), '매체'] = '구글'

    # 예외처리 정보 불러오기
    exception_sheet = spreadsheet.spread_sheet(doc, '예외처리')
    exception_sheet = exception_sheet.loc[(exception_sheet['체크박스']=='TRUE')&(exception_sheet['광고주']==tableau_info.account_name)]
    exception_sheet.index = range(0, len(exception_sheet))

    for i in exception_sheet.index:
        func = exception_sheet.iloc[i]['방식']
        col = exception_sheet.iloc[i]['구분']
        media = exception_sheet.iloc[i]['매체']
        name = exception_sheet.iloc[i]['이름']
        alt = exception_sheet.iloc[i]['대체 값']

        if func == 'EXCLUDE':
            raw_data = raw_data.loc[~((raw_data['매체']==media) & (raw_data[col]==name))]
        elif func == 'REPLACE' :
            raw_data.loc[((raw_data['매체'] == media) & (raw_data[col] == name)), name] = alt

    #creative_raw = creative_raw.loc[creative_raw['매체'].isin(['페이스북', '카카오', '구글'])]
    raw_data.loc[raw_data['매체']=='구글', '소재'] = raw_data['광고그룹']
    return raw_data

def asset_data_read(campaign_doc):
    # 애셋 정보 불러오기
    account_sheet = spreadsheet.spread_sheet(campaign_doc, 'Owner 정보')
    account_sheet = account_sheet.rename(columns = {'Owner ID' : 'owner_id'})
    account_sheet = account_sheet.drop_duplicates('owner_id')
    asset_data = pd.read_csv(tableau_info.asset_dir + f'/total_asset_data_{rdate.yearmonth}.csv')
    asset_data_merge = asset_data.merge(account_sheet, on ='owner_id', how='left')
    asset_data_merge = asset_data_merge.loc[asset_data_merge['광고주']==tableau_info.account_name]
    del asset_data_merge['owner_id'], asset_data_merge['광고주']
    return asset_data_merge

# 태블로 커스텀 정보 시트 불러오기
sheet_data = tableau_custom_sheet(document.doc)

# 원본 데이터 컬럼 <-> 태블로 템플릿용 데이터 컬럼 딕셔너리
column_dict = get_column_dict(sheet_data)

# 드롭박스에 적재된 광고주 리포트 RD 불러오기
raw_data = client_rd_read(column_dict, 'utf-8-sig')

# 캠페인 정보 시트 불러오기
campaign_sheet = campaign_info_sheet(document.campaign_doc)

# 리포트 RD 캠페인 정보 merge
raw_data = raw_data.merge(campaign_sheet, on=['매체', '캠페인', '광고그룹'], how='left')

# 데이터 예외처리
except_raw = data_exception(raw_data, document.doc)

# 소재 데이터 정보 불러오기
asset_data_merge = asset_data_read(document.campaign_doc)

# 리포트 RD <-> 소재 정보 merge
raw_merge_final = except_raw.merge(asset_data_merge, on = ['매체', '캠페인', '광고그룹', '소재'], how = 'left')

# 드롭박스 저장
raw_merge_final.to_csv(tableau_info.result_dir + f'/{tableau_info.account_name}/tableau_creative_rd_{tableau_info.result_name}_{rdate.yearmonth}.csv', index=False, encoding='utf-8-sig')