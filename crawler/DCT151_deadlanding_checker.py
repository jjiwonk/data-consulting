import setting.directory as dr

from spreadsheet import spreadsheet
import pandas as pd
import numpy as np
from selenium import webdriver
from pytz import timezone
import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

ADVERTISER = 'jobkorea'
GOOGLE_DOC = 'https://docs.google.com/spreadsheets/d/1B1wqyOMxqyShtSH0WboaQ-ozkFJ_ArtMA2jp-84kbTY/edit?pli=1#gid=1167985123'
ID = '확장소재_ID'
CHECKER = 'validation_checker'
URL = 'link1_url'


def get_url_check_list(ID, CHECKER, URL, GOOGLE_DOC):
    doc = spreadsheet.spread_document_read(GOOGLE_DOC)
    values = spreadsheet.spread_sheet(doc, 'url_check')
    url_check_list = values[[ID, CHECKER, URL]].replace('', np.nan).dropna().reset_index(drop=True)

    return url_check_list


def landing_check(url_check_list, ID, CHECKER, URL):
    result_df = pd.DataFrame(columns=[ID, CHECKER, URL])
    for index, row in url_check_list.iterrows():
        driver = webdriver.Chrome('D:/Github/data-consulting/crawler/chromedriver')
        url = row[URL]
        try:
            print(url)
            driver.get(url)
            driver.implicitly_wait(500)
            page_source = driver.page_source
            if '과도한 접속 시도' in page_source:
                print('ERROR: 잡코리아 접속 제한 이슈로 보안문자 입력 후 코드 재시작이 필요합니다.')
                break
            if row[CHECKER] in page_source:
                result_df = result_df.append(row)
        except Exception as e:
            print(f'{e}\n url 문제 발생:', url)
        driver.quit()

    return result_df

url_check_list = get_url_check_list(ID, CHECKER, URL, GOOGLE_DOC)
result_df = landing_check(url_check_list, ID, CHECKER, URL)
time = datetime.datetime.now(timezone("Asia/Seoul")).strftime("%y년_%m월_%d일_%H시")
result_df.to_csv(dr.download_dir+f'/{ADVERTISER}_link_validation_check_result_{time}.csv', index=False, encoding='utf-8-sig')