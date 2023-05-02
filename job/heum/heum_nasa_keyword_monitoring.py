from solution.DCT820_keyword_monitoring_solution import KeywordMonitoring

if __name__ == "__main__":
    worker = KeywordMonitoring(__file__)
    attr = dict(
        owner_id="heum", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        keyword_column="키워드",
        ad_names='{"heumtax.com": "혜움"}',
        error_slack_channel='heum_sa_입찰가솔루션',
        file_name='heum_keywords_list.csv',
        send_result_msg=False
    )

    worker.work(attr=attr, info=info)