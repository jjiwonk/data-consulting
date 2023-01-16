from datetime import datetime

from solution.cafe24_sales_monitor import Cafe24SalesMonitor
from utils.slack_bot import WEBHOOK_URL


if __name__ == "__main__":
    worker = Cafe24SalesMonitor(__file__)
    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), owner_id="bb", product_id="1950"
    )
    info = dict(
        cafe24_id="the1950jeju",
        cafe24_pw="dhtpals12!",
        store_name="1950",
        monitor_detail='false',
        slack_webhook_url=WEBHOOK_URL['1950_cafe24_매출_모니터링'],
    )

    worker.work(attr=attr, info=info)
