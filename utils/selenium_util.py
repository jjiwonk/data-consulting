from selenium import webdriver

from utils.os_util import is_mac_os, is_windows_os
from utils.path_util import get_resource


LINUX_CHROMEDRIVER = "chromedriver"
MAC_CHROMEDRIVER = "chromedriver-mac"
WINDOWS_CHROMEDRIVER = "chromedriver.exe"


def get_chromedriver(headless: bool = True, mobile: bool = False) -> webdriver.Chrome:
    driver_path = (
        get_resource(WINDOWS_CHROMEDRIVER)
        if is_windows_os()
        else get_resource(MAC_CHROMEDRIVER)
        if is_mac_os()
        else get_resource(LINUX_CHROMEDRIVER)
    )
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("window-size=1920x1080")
    chrome_options.add_argument("disable-gpu")
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

    return webdriver.Chrome(driver_path, options=chrome_options)
