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
        print(API_SERVICE_NAME, 'service created successfully')
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

    update_csv_sheet = openpyxl.load_workbook(update_csv_file).active.title
    xlApp = win32.Dispatch('Excel.Application')
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
    print('update successfully')


# 구글 시트 내용 지우기
def delete_gsheet(update_gsheet_url, update_gsheet_name, client_secret_file):
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    service = create_service(CLIENT_SECRET_FILE, API_SERVICE_NAME, API_VERSION, SCOPES)
    gsheet_id = update_gsheet_url.split('/')[5]
    response1 = service.spreadsheets().values().clear(
        spreadsheetId=gsheet_id,
        range=update_gsheet_name
    ).execute()
    print('delete successfully')