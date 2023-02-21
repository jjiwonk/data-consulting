from solution.DCT639_review_crawling_powderroom import PowderRoomCrawling
import datetime

from utils import s3

if __name__ == "__main__":
    worker = PowderRoomCrawling(__file__)

    attr = dict(
        schedule_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        owner_id="mediheal",
        product_id= "powderroom_crawling"
    )

    info = s3.get_info_from_s3(attr['owner_id'], attr['product_id'])
    worker.work(attr=attr, info=info)
