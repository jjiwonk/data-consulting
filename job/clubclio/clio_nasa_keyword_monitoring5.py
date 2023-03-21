from solution.DCT820_keyword_monitoring_solution import KeywordMonitoring

if __name__ == "__main__":
    worker = KeywordMonitoring(__file__)
    attr = dict(
        owner_id="clubclio", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        keyword_column="키워드",
        ad_names='{"clubclio.co.kr": "클리오", "www.ssg.com": "SSG닷컴", "oliveyoung.co.kr": "올리브영",'
                 ' "store.zigzag.kr": "지그재그", "coupang.com": "쿠팡", "gmarket.co.kr": "지마켓"}',
        slack_channel='gl_클리오_alert',
        error_slack_channel='gl_클리오_alert',
        file_name='clubclio_keywords_list5.csv',
        send_result_msg=False
    )

    worker.work(attr=attr, info=info)
