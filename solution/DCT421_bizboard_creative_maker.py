
from PIL import Image, ImageDraw, ImageFont
import gspread
import pandas as pd
import time
from oauth2client.service_account import ServiceAccountCredentials

dir = 'C:/Users/MADUP/Documents/지원/파이썬/데컨가자!/과제2/'
token_dir = 'C:/Users/MADUP/Dropbox (주식회사매드업)/광고사업부/데이터컨설팅/token'

def spread_document_read(url):
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    json_file = token_dir + '/madup-355605-cd37b0ac201f.json'
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

sheet_dir = spread_document_read('https://docs.google.com/spreadsheets/d/1IWLwjMOmQvP9RvPtwHfWeNUxeft6E7tzC-e8P3SRZHk/edit#gid=0')
title = spread_sheet(sheet_dir,'시트1', col_num=0, row_num=0)

font1 = ImageFont.truetype(dir + '폰트/SpoqaHanSansNeo-Bold.ttf', size=45)
font2 = ImageFont.truetype(dir + '폰트/SpoqaHanSansNeo-Regular.ttf', size=36)

def one_line_cre():
    background = Image.new('RGBA', (1029, 222))
    image = ImageDraw.Draw(background)
    image.text((48, 48), x['메인문구'], (76, 76, 76), font1)
    image.text((48, 111), x['서브문구(선택)'], (119, 119, 119), font2)
    image2 = Image.open(x['이미지경로'])
    image.paste(image2, (666, 0), image2)
    image.save('C:/Users/MADUP/Documents/지원/파이썬/데컨가자!/과제2/두줄test.png')
    return image


def two_line_cre():
    background = Image.new('RGBA', (1029, 222))
    image = ImageDraw.Draw(background)
    image.text((48, 81), x['메인문구'], (76, 76, 76), font1)
    image2 = Image.open(x['이미지경로'])
    image.paste(image2, (666, 0), image2)
    image.save('C:/Users/MADUP/Documents/지원/파이썬/데컨가자!/과제2/한줄test.png')
    return image

