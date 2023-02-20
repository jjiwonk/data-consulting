from datetime import datetime

from solution.naver_keyword_ad_monitor import NaverKeywordAdMonitor
from utils.slack_bot import WEBHOOK_URL

if __name__ == "__main__":
    worker = NaverKeywordAdMonitor(__file__)
    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), owner_id="bb", product_id="bb"
    )
    info = dict(
        keyword="abc주스",
        slack_webhook_url=WEBHOOK_URL['bb_latib_네이버sa_모니터링'],
        spreadsheet_url="https://docs.google.com/spreadsheets/d/1F519bvmhTftMmWT1IcxO5XdsuzqnqlE-9wLTuiVsbEo/edit#gid=0",
    )

    worker.work(info=info, attr=attr)
