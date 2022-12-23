from PIL import Image, ImageDraw, ImageFont
import gspread
import pandas as pd
import time
from oauth2client.service_account import ServiceAccountCredentials
from setting import directory as dr
from spreadsheet import spreadsheet

class directory :
    dropbox_dir = dr.dropbox_dir
    font_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 솔루션/비즈보드 소재 자동화/font'
class info :
    account_name = '광고주명'

    spread_url = 'https://docs.google.com/spreadsheets/d/1hFBpfIPWNuIXu4T8-5tqI7d32Ypr4TuQar21IHzc74A/edit#gid=0'
    biz_doc = spreadsheet.spread_document_read(spread_url)
    account_sheet = biz_doc.worksheet(account_name)

    creative_data = spreadsheet.spread_sheet(biz_doc, account_name, 0, 0)
    creative_data = creative_data.loc[creative_data['소재 생성']=='TRUE']

    pixel_dict = {
        '일반형': {
            '한줄형': {
                'main_text_x': 48,
                'main_text_y': 81,
            },
            '두줄형': {
                'main_text_x': 48,
                'main_text_y': 48,
                'sub_text_x': 48,
                'sub_text_y': 111
            }
        },
        '앱고지형': {}
    }

class font :
    main_font = ImageFont.truetype(directory.font_dir + '/Spoqa Han Sans Bold.ttf', size=45)
    sub_font = ImageFont.truetype(directory.font_dir + '/Spoqa Han Sans Regular.ttf', size=36)
    app_font = ImageFont.truetype(directory.font_dir + '/Spoqa Han Sans Regular.ttf', size=24)


def update_result(row_num, text):
    info.account_sheet.update('A' + str(row_num+1), False)
    info.account_sheet.update('B' + str(row_num+1), text)

def create_biz_banner(cre_info_dict):
    text_info = info.pixel_dict[cre_info_dict['배너 타입']][cre_info_dict['num_of_line']]

    default_image = Image.new('RGBA', (1029, 222))
    imdraw = ImageDraw.Draw(default_image)

    x = text_info['main_text_x']
    y = text_info['main_text_y']
    imdraw.text((x, y), cre_info_dict['메인문구'], (76, 76, 76), font.main_font)

    if cre_info_dict['num_of_line'] == '두줄형' :
        x = text_info['sub_text_x']
        y = text_info['sub_text_y']
        imdraw.text((x, y), cre_info_dict['서브문구'], (119, 119, 119), font.sub_font)

    if cre_info_dict['배너 타입'] == '앱고지형':
        pass

    object_image = Image.open(directory.dropbox_dir + cre_info_dict['오브제 이미지 경로'])

    default_image.paste(object_image, (666, 0), object_image)
    default_image.save(directory.dropbox_dir + cre_info_dict['파일 저장 경로'] + '/' + cre_info_dict['저장 파일명'])
    return default_image
def minimum_constraint_test(test_image):
    crop_image = test_image.crop((290, 0, 585, 222))
    crop_image_bbox = crop_image.getbbox()
    return crop_image_bbox == None

def maximum_constraint_test(test_image):
    crop_image = test_image.crop((585, 0, 665, 222))
    crop_image_bbox = crop_image.getbbox()
    return crop_image_bbox != None

def do_work():
    creative_data = info.creative_data.reset_index()

    for i in range(len(creative_data)):
        row = creative_data.iloc[i]
        cre_info_dict = row.to_dict()

        if row['서브문구'] == '' :
            cre_info_dict['num_of_line'] = '한줄형'
        else :
            cre_info_dict['num_of_line'] = '두줄형'

        try :
            image = create_biz_banner(cre_info_dict)
            if minimum_constraint_test(image) == True:
                result_text = 'WARNING : 해당 배너는 최소 글자 길이 제한을 만족하지 못합니다.'
            elif maximum_constraint_test(image) == True:
                result_text = 'WARNING : 해당 배너는 최대 글자 길이 제한을 초과하였습니다.'
            else:
                result_text = 'SUCCESS : 소재 생성을 완료하였습니다.'
        except :
            result_text = 'FAIL : 이미지 생성 중 오류가 발생했습니다. 입력 정보를 다시 확인해주세요.'

        update_result(row['index'], result_text)