import os

from solution.DCT555_shopping_center_solution import SpcDownload
from datetime import datetime
from utils import dropbox_util

if __name__ == "__main__":
    worker = SpcDownload(__file__)
    def get_info():
        token_dir = 'C:/Users/MADUP/Dropbox (주식회사매드업)/광고사업부/데이터컨설팅/데이터 솔루션/쇼핑파트너센터 다운 자동화'

        f = open(token_dir + '/spc_download_info_siv.txt', 'r')
        info = eval(f.read())
        return info

    attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        owner_id="SIV",
        product_id= "spc_download"
    )
    info = get_info()

    worker.work(attr=attr, info=info)
