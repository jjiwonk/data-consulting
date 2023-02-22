from PIL import Image, ImageDraw, ImageFont
from setting import directory as dr
from spreadsheet import spreadsheet

class directory :
    dropbox_dir = dr.dropbox_dir
    font_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 솔루션/비즈보드 소재 자동화/font'
class info :
    account_name = '29CM'

    spread_url = 'https://docs.google.com/spreadsheets/d/1hFBpfIPWNuIXu4T8-5tqI7d32Ypr4TuQar21IHzc74A/edit#gid=0'
    biz_doc = spreadsheet.spread_document_read(spread_url)
    account_sheet = biz_doc.worksheet(account_name)

    creative_data = spreadsheet.spread_sheet(biz_doc, account_name, 0, 0)
    creative_data = creative_data.loc[creative_data['소재 생성']=='TRUE']

    pixel_dict = {
        '일반형': {
            '한줄형': {
                'main_text_x': 48,
                'main_text_y': 106,
            },
            '두줄형': {
                'main_text_x': 48,
                'main_text_y': 77,
                'sub_text_x': 48,
                'sub_text_y': 144
            }
        },
        '앱고지형':  {
            '한줄형': {
                'app_text_x' : 95,
                'app_text_y' : 78,
                'main_text_x': 48,
                'main_text_y': 127,
            },
            '두줄형': {
                'app_text_x': 95,
                'app_text_y': 50,
                'main_text_x': 48,
                'main_text_y': 99,
                'sub_text_x': 48,
                'sub_text_y': 163
            }
        }
    }

class font :
    main_font = ImageFont.truetype(directory.font_dir + '/Spoqa Han Sans Bold.ttf', size=48)
    sub_font = ImageFont.truetype(directory.font_dir + '/Spoqa Han Sans Regular.ttf', size=39)
    app_font = ImageFont.truetype(directory.font_dir + '/Spoqa Han Sans Regular.ttf', size=24)

class banner :
    def __init__(self, default_image):
        self.default_image = default_image
        self.imdraw = ImageDraw.Draw(default_image)
    def get_real_y_of_text(self, object_y, target_font, text):
        object_height = target_font.getmask(text).getbbox()[3]
        actual_height = target_font.getmetrics()[0] + target_font.getmetrics()[1]

        result = object_y - ((actual_height - object_height) / 2)

        return result

    def draw_text(self, x , y, input_text, font, color):
        y = self.get_real_y_of_text(y,font, input_text)
        self.imdraw.text((x, y), input_text, color, font)


def update_result(row_num, text):
    info.account_sheet.update('A' + str(row_num+1), False)
    info.account_sheet.update('B' + str(row_num+1), text)

def create_biz_banner(cre_info_dict):
    text_info = info.pixel_dict[cre_info_dict['배너 타입']][cre_info_dict['num_of_line']]

    default_image = Image.new('RGBA', (1029, 258))
    biz_banner = banner(default_image)

    if cre_info_dict['배너 타입'] == '앱고지형':
        # 앱 로고 이미지
        app_logo_image = Image.open(directory.dropbox_dir + cre_info_dict['앱 로고 경로']).convert("RGBA")

        if cre_info_dict['num_of_line'] == '한줄형' :
            app_y = 71
        elif cre_info_dict['num_of_line'] == '두줄형' :
            app_y = 43
        biz_banner.default_image.paste(app_logo_image, (48, app_y), app_logo_image)

        # 앱 랜딩 고지문
        biz_banner.draw_text(x = text_info['app_text_x'], y = text_info['app_text_y'], input_text = cre_info_dict['앱 랜딩 고지문'], font = font.app_font, color = (119, 119, 119))
        arrow_x = biz_banner.default_image.getbbox()[2]

        # 화살표 넣기
        arrow = Image.open(directory.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 솔루션/비즈보드 소재 자동화/object/비즈보드_화살표.png')
        biz_banner.default_image.paste(arrow, (arrow_x + 6, app_y + 13), arrow)

    biz_banner.draw_text(x = text_info['main_text_x'], y = text_info['main_text_y'], input_text = cre_info_dict['메인문구'], font = font.main_font, color = (76, 76, 76))

    if cre_info_dict['num_of_line'] == '두줄형' :
        biz_banner.draw_text(x=text_info['sub_text_x'], y=text_info['sub_text_y'], input_text=cre_info_dict['서브문구'],
                             font=font.sub_font, color=(119, 119, 119))

    object_image = Image.open(directory.dropbox_dir + cre_info_dict['오브제 이미지 경로'])

    biz_banner.default_image.paste(object_image, (666, 0), object_image)
    biz_banner.default_image.save(directory.dropbox_dir + cre_info_dict['파일 저장 경로'] + '/' + cre_info_dict['저장 파일명'])
    return biz_banner.default_image
def minimum_constraint_test(test_image):
    crop_image = test_image.crop((290, 0, 585, 258))
    crop_image_bbox = crop_image.getbbox()
    return crop_image_bbox == None

def maximum_constraint_test(test_image):
    crop_image = test_image.crop((585, 0, 665, 258))
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

do_work()
