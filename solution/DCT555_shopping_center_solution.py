from selenium import webdriver
from selenium.webdriver.common.by import By
from setting import directory as dr
import os
import pandas as pd
from datetime import date

drop_dir = 'C:/Users/MADUP/Dropbox (주식회사매드업)/광고사업부/데이터컨설팅/데이터 솔루션/쇼핑파트너센터 다운 자동화'
target_date = date.today()

f = open(drop_dir + '/spc_login.txt', 'r')
login_info = f.read()
login_info = eval(login_info)

brand = login_info['brand']

def selenium_download():
    driver = webdriver.Chrome("C:/Users/MADUP/Dropbox (주식회사매드업)/광고사업부/데이터컨설팅/token/chromedriver.exe")
    driver.get('https://center.shopping.naver.com')

    # 로그인하기

    id_input = driver.find_element(by=By.ID, value='normal_login_username')
    id_input.send_keys(login_info['id'])

    pw_input = driver.find_element(by=By.ID, value='normal_login_password')
    pw_input.send_keys(login_info['pw'])

    login_click = driver.find_element(by=By.CSS_SELECTOR,value='#root > div > div > div > div > form > div:nth-child(4) > div > div > span > button.ant-btn.btn_main_login.ant-btn-primary')
    login_click.click()

    driver.implicitly_wait(time_to_wait=5)

    # 상품 탭 가기

    service_item = driver.find_element(by=By.CSS_SELECTOR, value='#content > div.tab2 > ul > li:nth-child(2) > a')
    service_item.click()

    # 상품수 계산

    def down_num():
        total_item = driver.find_element(by=By.CSS_SELECTOR, value='#content > div.prdt_status_lst > h4 > a > span')
        total_item = total_item.text.replace(',', '')
        down_num = int(total_item) / 1000
        return int(down_num) + 1

    down_num = down_num()

    # 엑셀 다운받기

    excel_down = driver.find_element(by=By.CSS_SELECTOR, value='#excelDown > a')
    excel_down.click()

    total_excel_down = driver.find_element(by=By.CSS_SELECTOR, value='#excelDown > div > ul > li:nth-child(1) > a')
    total_excel_down.click()

    # 팝업창으로 바꿔주기

    driver.switch_to.window(driver.window_handles[-1])

    start_down = driver.find_element(by=By.CSS_SELECTOR, value='#downloadList > tr > td.last > a')
    start_down.click()
    driver.implicitly_wait(time_to_wait=3)

    try:
        for i in range(1, down_num):
            driver.implicitly_wait(time_to_wait=5)
            file_grp = driver.find_element(by=By.CSS_SELECTOR,value=f'#downloadList > tr:nth-child({i}) > td:nth-child(3) > span > span')
            percent = file_grp.get_attribute('style')

            if percent == 'width: 100%;':
                file_down = driver.find_element(by=By.CSS_SELECTOR,value=f'#downloadList > tr:nth-child({i+1}) > td.last > a')
                file_down.click()
            else:
                driver.implicitly_wait(time_to_wait=5)
                file_down = driver.find_element(by=By.CSS_SELECTOR,value=f'#downloadList > tr:nth-child({i+1}) > td.last > a')
                file_down.click()
        result = 'file read success'

    except Exception as e:
        result = 'file read failed'

    return result

result = selenium_download()

def file_conat():
    if result == 'file read success':

        file_dir = dr.download_dir
        files = os.listdir(file_dir)
        files = [f for f in files if '서비스_상품' in f]

        item_df = pd.DataFrame()

        for f in files:
            df = pd.read_excel(file_dir + '/' + f, header = 1)
            item_df = pd.concat([item_df,df]).fillna('')

        item_df.to_csv(drop_dir + f'/{brand}_EP_item_list_{target_date}.csv', index= False , encoding= 'utf-8-sig')

        for f in files:
            os.remove(file_dir + '/' + f)

    else :
        print('file concat fail')

    return item_df

df = file_conat()





