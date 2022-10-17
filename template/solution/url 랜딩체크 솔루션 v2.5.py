# !pip install selenium
# !pip install datetime
# !pip install warnings
# !pip install webdriver_manager
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
import numpy as np
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from pytz import timezone
import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import time
start = time.time()

class directory:
    dir_list = os.getcwd().split('\\')
    dropbox_dir = '/'.join(dir_list[:dir_list.index('Dropbox (주식회사매드업)') + 1])
    # 드롭박스 폴더 위치경로

    download_dir = 'C:/Users/MADUP/Downloads'
    # 최종 결과파일을 다운받을 폴더 위치경로

class spreadsheet:
    def spread_document_read(self, url):
        token_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/token'
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        json_file = token_dir + '/madup-355605-cd37b0ac201f.json'
        credentials = ServiceAccountCredentials.from_json_keyfile_name(json_file, scope)
        gc = gspread.authorize(credentials)
        read = gc.open_by_url(url)
        print('스프레드시트 읽기 완료')
        return read

    def spread_sheet(self, doc, sheet_name, col_num=0, row_num=0):
        while True:
            try:
                data_sheet = doc.worksheet(sheet_name)
                data_sheet_read = data_sheet.get_all_values()
                result = pd.DataFrame(data_sheet_read, columns=data_sheet_read[row_num]).iloc[row_num + 1:, col_num:]
            except:
                print('API 오류로 15초 후 다시 시도 합니다.')
                time.sleep(15)
                continue
            break
        return result


spreadsheet = spreadsheet()
dr = directory()
doc = spreadsheet.spread_document_read('https://docs.google.com/spreadsheets/d/14i42NrpnA_9k8nCgyc4Y5KssLP8tOOXUyzYw0un7qXY/edit#gid=211027705')
ADVERTISER = '무신사'
result_dir = dr.download_dir


def get_url_check_list(doc, ADVERTISER):
    url_check_list = spreadsheet.spread_sheet(doc, ADVERTISER).reset_index(drop=True)
    url_check_list = url_check_list[['id', 'url']].replace('', np.nan).dropna().reset_index(drop=True)

    return url_check_list


def link_validation_check(url_check_list, checker):
    result_df = pd.DataFrame(columns=['id', 'url', 'checker'])
    options = webdriver.ChromeOptions()
    options.add_argument('headless')
    options.add_argument('window-size=1920x1080')
    options.add_argument("disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36")

    driver = webdriver.Chrome(executable_path=dr.dropbox_dir + '/광고사업부/데이터컨설팅/token/chromedriver', options=options)
    recheck_list = pd.DataFrame(columns=['id', 'url'])
    for index, row in url_check_list.iterrows():
        url = row['url']
        try:
            print(url)
            driver.get(url)
            page_source = driver.page_source
            for val in checker:
                if val in page_source:
                    row['checker'] = val
                    result_df = result_df.append(row)
        except Exception as e:
            if 'timeout' in str(e):
                print(f'{e}\n url 문제 발생:', url)
                recheck_list = recheck_list.append(row)
                driver.quit()
                driver = webdriver.Chrome(executable_path=dr.dropbox_dir + '/광고사업부/데이터컨설팅/token/chromedriver', options=options)
                continue
            if 'Alert' in str(e):
                row['checker'] = 'alert'
                result_df = result_df.append(row)
                continue

    for index, row in recheck_list.iterrows():
        url = row['url']
        try:
            print(url)
            driver.get(url)
            page_source = driver.page_source
            for val in checker:
                if val in page_source:
                    row['checker'] = val
                    result_df = result_df.append(row)
        except Exception as e:
            print(f'{e}\n url 문제 발생:', url)

    driver.quit()

    return result_df


def landing_expired_check(url_check_list):
    landing_expired_result_df = pd.DataFrame(columns=['id', 'url'])
    return landing_expired_result_df


def redirect_check(url_check_list):
    redirect_result_df = pd.DataFrame(columns=['id', 'url'])
    return redirect_result_df


def download_df(link_validation_result_df, landing_expired_result_df, redirect_result_df, result_dir):
    time = datetime.datetime.now(timezone("Asia/Seoul")).strftime("%y년_%m월_%d일_%H시")
    writer = pd.ExcelWriter(result_dir + f'/{ADVERTISER}_랜딩페이지_점검_{time}.xlsx', engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}})
    link_validation_result_df.to_excel(writer, sheet_name = 'link_validation_check', index=False)
    # landing_expired_result_df.to_excel(writer, sheet_name='landing_expired_check', index=False)
    # redirect_result_df.to_excel(writer, sheet_name='redirect_check', index=False)
    writer.close()


def landing_check_solution_exec(doc, ADVERTISER):
    url_check_list = get_url_check_list(doc, ADVERTISER)

    total_index = spreadsheet.spread_sheet(doc, 'TOTAL')
    total_index = total_index.drop_duplicates('업체명', keep='last')
    index = total_index.loc[total_index['업체명'] == ADVERTISER].reset_index(drop=True)

    link_validation_result_df = pd.DataFrame(columns=['id', 'url'])
    landing_expired_result_df = pd.DataFrame(columns=['id', 'url'])
    redirect_result_df = pd.DataFrame(columns=['id', 'url'])
    if index['문장 패턴 검색'].values == 'TRUE':
        checker = index['검색 문구'][0].split('/')
        link_validation_result_df = link_validation_check(url_check_list, checker)
    elif index['접속 불가 점검(미구현)'].values == 'TRUE':
        landing_expired_result_df = landing_expired_check(url_check_list)
    elif index['리다이렉트 점검(미구현)'].values == 'TRUE':
        redirect_result_df = redirect_check(url_check_list)

    download_df(link_validation_result_df, landing_expired_result_df, redirect_result_df, result_dir)
    print('download success')
    print('time :', time.time() - start)


landing_check_solution_exec(doc, ADVERTISER)