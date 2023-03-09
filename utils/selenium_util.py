import logging
import os
import time
from time import sleep

from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement

from utils.os_util import is_mac_os, is_windows_os
from utils.path_util import get_resource
from utils import s3
from utils import const

from selenium.common.exceptions import (
    StaleElementReferenceException,
    TimeoutException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.common.by import By



LINUX_CHROMEDRIVER = "chromedriver"
MAC_CHROMEDRIVER = "chromedriver-mac"
WINDOWS_CHROMEDRIVER = "chromedriver.exe"


def get_chromedriver(headless: bool = True, mobile: bool = False, download_dir: str = "/tmp", user_agent: str = None) -> webdriver.Chrome:
    driver_path = (
        get_resource(WINDOWS_CHROMEDRIVER)
        if is_windows_os()
        else get_resource(MAC_CHROMEDRIVER)
        if is_mac_os()
        else get_resource(LINUX_CHROMEDRIVER)
    )
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--window-size=1920x1080")
    chrome_options.add_argument("--width=1920")
    chrome_options.add_argument("--height=1080")
    chrome_options.add_argument("disable-gpu")  # 가속 사용 x
    chrome_options.add_argument("lang=ko_KR")  # 가짜 플러그인 탑재
    chrome_options.add_argument("--ignore-certificate-errors")
    if user_agent is None:
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/110.0.5481.78 Safari/537.36")
    else:
        chrome_options.add_argument(
            f"user-agnet={user_agent}"
        )

    if headless is True:
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("disable-component-cloud-policy")
    if mobile is True:
        mobile_emulation = {"deviceName": "iPhone X"}
        chrome_options.add_experimental_option("mobileEmulation", mobile_emulation)
    if download_dir:
        prefer_ = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "profile.default_content_settings.popups": 0,
            "directory_upgrade": True,
            # 'safebrowsing.enabled': True
        }
        chrome_options.add_experimental_option("prefs", prefer_)

    return webdriver.Chrome(driver_path, options=chrome_options)


# 다운로드 버튼 클릭 직전의 파일목록과 클릭 이후 파일 목록을 비교하여
# 다운받은 파일 명을 찾아 리턴
def click_and_find_downloaded_filename(
    *, clickable_btn: WebElement, download_dir: str = "/tmp", download_file_ext: str = None, wait_sec: int = 60
):
    before_snapshot = _get_file_list(target_dir=download_dir, target_file_ext=download_file_ext)

    clickable_btn.click()

    elapsed_sec = 0
    while elapsed_sec < wait_sec:
        elapsed_sec += 1
        sleep(1)

        after_snapshot = _get_file_list(target_dir=download_dir, target_file_ext=download_file_ext)

        if len(before_snapshot) < len(after_snapshot):
            return (set(after_snapshot) - set(before_snapshot)).pop()

    raise Exception(f"download timeout({wait_sec} sec)")


def _get_file_list(*, target_dir: str = "/tmp", target_file_ext: str = None):
    if target_file_ext:
        return [f for f in os.listdir(target_dir) if f.endswith(target_file_ext)]
    else:
        return os.listdir(target_dir)

def wait_for_element(driver, value, by=By.CSS_SELECTOR, max_retry_cnt = 3):
    while max_retry_cnt >= 0:
        try:
            elements = WebDriverWait(
                driver,
                10,
            ).until(expected_conditions.presence_of_all_elements_located((by, value)))
            if len(elements) == 1:
                return elements[0]
            else:
                return elements
        except StaleElementReferenceException or TimeoutException as e:
            logging.warning(e)
            if max_retry_cnt > 0:
                max_retry_cnt -= 1
                driver.refresh()
                time.sleep(1)
            else:
                raise e

def selenium_error_logging(driver, download_dir, screenshot_file_name, page_source_file_name) :
    local_screenshot_path = download_dir + '/' + screenshot_file_name
    driver.get_screenshot_as_png()
    driver.save_screenshot(download_dir + '/' + screenshot_file_name)
    s3.upload_file(local_path=local_screenshot_path, s3_path='screenshot/' + screenshot_file_name,
                   s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET)
    os.remove(local_screenshot_path)

    page_source_path = download_dir + '/' + page_source_file_name
    page_source = driver.page_source
    f = open(page_source_path, 'w', encoding='UTF-8')
    f.write(page_source)
    f.close()
    s3.upload_file(local_path=page_source_path, s3_path='page_source/' + page_source_file_name,
                   s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET)
    os.remove(page_source_path)