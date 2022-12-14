import sys
sys.path.append('home/ec2-user/data-consulting')

import setting.ec2_directory as dr
import spreadsheet.ec2_spreadsheet as spreadsheet
import os.path
import pandas as pd
import numpy as np
from selenium import webdriver
import chromedriver_autoinstaller as auto
from pytz import timezone
import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
import time
start = time.time()

doc = spreadsheet.spread_document_read('https://docs.google.com/spreadsheets/d/14i42NrpnA_9k8nCgyc4Y5KssLP8tOOXUyzYw0un7qXY/edit#gid=211027705')
ADVERTISER = '무신사'
result_dir = dr.ec2_dir

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

    chrome_ver = auto.get_chrome_version().split('.')[0]
    driver_path = f'./{chrome_ver}/chromedriver.exe'
    if os.path.exists(driver_path):
        print(f'chrome driver is installed: {driver_path}')
    else:
        print(f'install the chrome driver(ver: {chrome_ver})')
        auto.install(True)
    driver = webdriver.Chrome(driver_path, options=options)
    # driver = webdriver.Chrome(executable_path=dr.ec2_dir + '/token/chromedriver', options=options)
    # driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)
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
                driver = webdriver.Chrome(driver_path, options=options)
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


