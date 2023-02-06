from solution.DCT555_shopping_center_solution import SpcDownload
from datetime import datetime

from utils import s3

if __name__ == "__main__":
    worker = SpcDownload(__file__, DOWLOAD_SKIP = True)

    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        owner_id="siv",
        product_id= "spc_download"
    )

    info = s3.get_info_from_s3(attr['owner_id'], attr['product_id'])
    worker.work(attr=attr, info=info)
