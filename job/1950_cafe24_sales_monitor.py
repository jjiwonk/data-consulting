from datetime import datetime

from solution.cafe24_sales_monitor import Cafe24SalesMonitor
from solution.cafe24_sales_monitor import Key
from utils.slack_bot import WEBHOOK_URL


if __name__ == "__main__":
    set = dict(
        cafe24_id="the1950jeju",
        cafe24_pw="dhtpals12!",
        store_name="1950",
        monitor_detail='false',
        slack_webhook_url=WEBHOOK_URL['1950_cafe24_매출_모니터링'],
        # slack_mention_id="UN9Q07FQB",
        # spreadsheet_url="https://docs.google.com/spreadsheets/d/1I1J4LStGRKyT_TUE-cFXon9DZwt4kYg_V69orgRcSPI/edit#gid=0",
        # raw_sheet_name="RD",
        # sales_sheet_name="Summary",
        # n_products="10"
    )
    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), owner_id="bb", product_id="1950"
    )
    # Key.USE_HEADLESS = False
    # from madup_argo.core.util.log import change_level_info

    # change_level_info()
    worker = Cafe24SalesMonitor(attr, set)
    print(worker.do_work(attr, set))