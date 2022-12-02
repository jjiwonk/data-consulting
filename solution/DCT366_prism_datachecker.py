import spreadsheet.spreadsheet as sp
import pandas as pd
import setting.directory as dr
import os

#정합성 체크를 할 데이터 불러오기

def read_file (rd):
    file_path = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 솔루션/정합성 체크 솔루션/' +rd
    file_list = os.listdir(file_path)
    raw_data = pd.DataFrame()
    for file in file_list:
        file_df = pd.read_csv(file_path + '/' + file, header=0, encoding='utf-8-sig')
        raw_data = pd.concat([raw_data, file_df])
    return raw_data

df = read_file('원본데이터')
df2 = read_file('비교데이터')

sheet_dir = 'https://docs.google.com/spreadsheets/d/11-TknpD_HzxZ-N7sVEJrS7iEkJcUz4xzJVL0YQPrvZY/edit#gid=0'
sheet = sp.spread_document_read(sheet_dir)

def read_sheet(sheet,sheet_name):
    df = sp.spread_sheet(sheet, sheet_name)
    return df

df_col = read_sheet(sheet,'columns')

# 데이터들을 비교하기 위해 컬럼 정리

df_col = df_col.loc[df_col['field']!='none']
col_dic = dict(zip(list(df_col['rep']), list(df_col['api'])))

metric_list = df_col.loc[df_col['field'] == 'metric','api'].to_list()
dimension_list = df_col.loc[df_col['field'] == 'dimension','api'].to_list()

df = df.rename(columns = col_dic)

def get_pivot(df, df2):
    df[metric_list] = df[metric_list].apply(lambda x: pd.to_numeric(x.str.replace(' ','')))
    df2[metric_list] = df2[metric_list].apply(lambda x: x.replace(' ','').astype(float) * -1)
    df_concat = pd.concat([df, df2], ignore_index= True)
    final_df = df_concat.pivot_table(index = dimension_list, values = metric_list, aggfunc = 'sum').reset_index()
    return final_df

df_f = get_pivot(df, df2)
df_f.to_excel(dr.download_dir + '/정합성체크데이터.xlsx', encoding='utf-8-sig', index= False)

# 결과 파일을 스프레드 시트로 업데이트함

import win32com.client as win32
import openpyxl
import datetime, json
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

def create_service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    cred = ServiceAccountCredentials.from_json_keyfile_name(CLIENT_SECRET_FILE, SCOPES)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        # print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print(e)
    return None


def json_default(value):
    if isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    raise TypeError('not JSON serializable')


def update_gsheet(update_gsheet_url, update_gsheet_name, client_secret_file, update_csv_file):
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    service = create_service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)

    update_csv_sheet = openpyxl.load_workbook(update_csv_file).active.title #액셀파일 접근, 시트 1개일때
    xlApp = win32.Dispatch('Excel.Application') #엑셀 프로그램 실행
    wb = xlApp.Workbooks.Open(update_csv_file)
    xlApp.Visible = False  # 실행과정 안보이게
    ws = wb.Worksheets(update_csv_sheet)
    rngData = ws.Range('A1').CurrentRegion()
    rngData = json.dumps(rngData, default=json_default)
    rngData = json.loads(rngData)
    gsheet_id = update_gsheet_url.split('/')[5]
    response1 = service.spreadsheets().values().clear(
        spreadsheetId=gsheet_id,
        range=update_gsheet_name
    ).execute()
    response2 = service.spreadsheets().values().update(
        spreadsheetId=gsheet_id,
        valueInputOption='RAW',
        range=update_gsheet_name + '!A1',
        body=dict(
            majorDimension='ROWS',
            values=rngData
        )
    ).execute()
    wb.Close()
    xlApp.quit()
    print('gsheet update successfully')

update_gsheet_url = sheet_dir
update_gsheet_name = '결과'

token_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/token'
client_secret_file = token_dir + '/madup-355605-cd37b0ac201f.json'
update_csv_file = dr.download_dir + '/정합성체크데이터.xlsx'

update_gsheet(update_gsheet_url, update_gsheet_name, client_secret_file, update_csv_file)















