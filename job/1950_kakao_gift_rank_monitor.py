from datetime import datetime

from solution.kakao_gift_ranking_collect import KakaoGiftRankingCollect
from solution.kakao_gift_ranking_collect import Key
from utils.slack_bot import WEBHOOK_URL

if __name__ == "__main__":
    test_set = dict(
        brand_name="1950",
        slack_webhook_url=WEBHOOK_URL['bb_1950_카카오선물하기_랭킹모니터링'],
        spreadsheet_url="https://docs.google.com/spreadsheets/d/109SDT_tbvGRPOs0zfRDhLbD9cR-wIfkKllkfj6mMicQ/edit#gid=0",
        sheet_name = "시트1",
        # slack_mention_id="UN9Q07FQB",
        search_keywords="1950, 치약",
    )
    test_attr = dict(
        schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'), owner_id="bb", product_id="1950"
    )

    # Key.USE_HEADLESS = False
    # from madup_argo.core.util.log import change_level_info

    # change_level_info()
    worker = KakaoGiftRankingCollect(test_attr, test_set)
    print(worker.do_work(test_attr, test_set))