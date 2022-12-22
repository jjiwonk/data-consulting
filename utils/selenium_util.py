import logging
import os
import time
from time import sleep

from selenium import webdriver
from selenium.webdriver.remote.webelement import WebElement

from utils.os_util import is_mac_os, is_windows_os
from utils.path_util import get_resource


LINUX_CHROMEDRIVER = "chromedriver"
MAC_CHROMEDRIVER = "chromedriver-mac"
WINDOWS_CHROMEDRIVER = "chromedriver.exe"


def get_chromedriver(headless: bool = True, mobile: bool = False, download_dir: str = "/tmp") -> webdriver.Chrome:
    driver_path = (
        get_resource(WINDOWS_CHROMEDRIVER)
        if is_windows_os()
        else get_resource(MAC_CHROMEDRIVER)
        if is_mac_os()
        else get_resource(LINUX_CHROMEDRIVER)
    )
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("window-size=1920x1080")
    chrome_options.add_argument("disable-gpu") # 가속 사용 x
    chrome_options.add_argument("lang=ko_KR")  # 가짜 플러그인 탑재
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/61.0.3163.100 Safari/537.36"
    )

    if headless is True:
        chrome_options.add_argument("headless")
        chrome_options.add_argument("--no-sandbox")
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