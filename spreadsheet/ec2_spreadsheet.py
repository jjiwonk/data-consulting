from oauth2client.service_account import ServiceAccountCredentials
import setting.ec2_directory as edr

import gspread
import pandas as pd
import time

ec2_dir = edr.ec2_dir + '/token'

def spread_document_read(url):
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    json_file = ec2_dir + '/madup-355605-cd37b0ac201f.json'
    credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file, scope)
    gc = gspread.authorize(credentials)
    read = gc.open_by_url(url)
    print('스프레드시트 읽기 완료')
    return read

def spread_sheet(doc, sheet_name, col_num=0, row_num=0):
    while True :
        try :
            data_sheet = doc.worksheet(sheet_name)
            data_sheet_read = data_sheet.get_all_values()
            result = pd.DataFrame(data_sheet_read, columns=data_sheet_read[row_num]).iloc[row_num+1:, col_num:]
        except :
            print('API 오류로 15초 후 다시 시도 합니다.')
            time.sleep(15)
            continue
        break
    return result