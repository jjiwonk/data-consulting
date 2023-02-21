from datetime import datetime

from solution.cafe24_sales_monitor import Cafe24SalesMonitor
from utils import s3

if __name__ == "__main__":
    worker = Cafe24SalesMonitor(__file__)
    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), owner_id="bb", product_id="1950"
    )
    info = s3.get_info_from_s3(attr['owner_id'], attr['product_id'])
    info['error_slack_channel'] = 'bb_data_에러모니터링'

    worker.work(attr=attr, info=info)