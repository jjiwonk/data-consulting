from datetime import datetime

from utils import s3

if __name__ == "__main__":
    #worker = NaverShoppingCrawling(__file__)

    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        owner_id="mediheal",
        product_id= "review_data_report"
    )

    info = s3.get_info_from_s3(attr['owner_id'], attr['product_id'])
    #worker.work(attr=attr, info=info)
