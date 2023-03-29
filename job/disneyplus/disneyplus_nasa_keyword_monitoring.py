from solution.DCT820_keyword_monitoring_solution import KeywordMonitoring

if __name__ == "__main__":
    worker = KeywordMonitoring(__file__)
    attr = dict(
        owner_id="disneyplus", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        keyword_column="키워드",
        ad_names='{"disneyplus": "디즈니플러스"}',
        error_slack_channel='gl_글로벌_디즈니플러스_alert',
        file_name='disneyplus_keywords_list.csv',
        send_result_msg=False
    )

    worker.work(attr=attr, info=info)
