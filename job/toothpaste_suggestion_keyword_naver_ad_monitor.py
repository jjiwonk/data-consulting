from datetime import datetime

from solution.naver_keyword_ad_monitor import NaverKeywordAdMonitor
from solution.naver_keyword_ad_monitor import Key
from utils.slack_bot import WEBHOOK_URL

if __name__ == "__main__":
    test_set = dict(
        keyword="치약추천",
        powerlink_ad_cnt_pc="5",
        powerlink_ad_cnt_mo="3",
        slack_webhook_url=WEBHOOK_URL['bb_1950_네이버sa_모니터링'],
        spreadsheet_url="https://docs.google.com/spreadsheets/d/1i-f55AOXVBNfUKlHnfc8_jw54Kc2s3D3B-fsWSVoSvM/edit#gid=0",
    )
    test_attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), owner_id="bb", product_id="bb"
    )
    # Key.USE_HEADLESS = False
    worker = NaverKeywordAdMonitor(test_attr, test_set)
    print(worker.do_work(test_attr, test_set))