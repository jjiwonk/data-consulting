from datetime import datetime

from solution.cafe24_sales_monitor import Cafe24SalesMonitor
from utils.slack_bot import WEBHOOK_URL


if __name__ == "__main__":
    worker = Cafe24SalesMonitor(__file__)
    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), owner_id="bb", product_id="latib"
    )
    info = dict(
        cafe24_id="latib",
        cafe24_pw="fkxlqm2022*!",
        store_name="latib",
        slack_webhook_url=WEBHOOK_URL['bb_latib_매출_모니터링'],
        spreadsheet_url="https://docs.google.com/spreadsheets/d/1I1J4LStGRKyT_TUE-cFXon9DZwt4kYg_V69orgRcSPI/edit#gid=0",
        raw_sheet_name="RD",
        sales_sheet_name="Summary"
    )

    worker.work(attr=attr, info=info)
