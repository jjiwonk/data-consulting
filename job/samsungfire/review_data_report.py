from solution.DCT1032_custom_review_data_report import ReviewDataReport
from datetime import datetime
import os

from utils import s3
from utils import const
from utils import os_util
from utils.path_util import get_tmp_path
import pandas as pd

if __name__ == "__main__":
    worker = ReviewDataReport(__file__)

    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        owner_id="samsungfire",
        product_id= "review_data_report"
    )

    info = dict(
        slack_channel = "dc_민정수_test",
        error_slack_channel = "dc_민정수_test",
        replace_word_list = ['\[모바일\(Android\)\]', '\[모바일\(iOS\)\]', '\[모바일\(Chrome OS\)\]']
    )

    tmp_path = get_tmp_path() + "/" + attr['owner_id'] + "/" + attr['product_id']
    os.makedirs(tmp_path, exist_ok=True)

    if os_util.is_windows_os():
        tmp_path = tmp_path.replace('/', '\\')

    raw_data_dir = s3.download_file(s3_path = 'review_data/owner_id=samsungfire/samsungfire_review_data.csv',
                                s3_bucket= const.DEFAULT_S3_PRIVATE_BUCKET,
                                local_path= tmp_path)
    os.remove(raw_data_dir)

    raw_data = pd.read_csv(raw_data_dir)
    raw_data = raw_data.drop_duplicates(['제품명', 'review_text', 'user_name', 'review_date'])

    info['tmp_path'] = tmp_path
    info['raw_data'] = raw_data

    worker.work(attr=attr, info=info)
