from solution.DCT820_keyword_monitoring_solution import KeywordMonitoring

if __name__ == "__main__":
    worker = KeywordMonitoring(__file__)
    attr = dict(
        owner_id="29cm", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        keyword_column="키워드",
        ad_names='["29cm", "29CM"]',
        slack_channel='dcbot_29cm_네이버sa_키워드순위모니터링',
        error_slack_channel='dcbot_29cm_네이버sa_키워드순위모니터링',
        file_name='29cm_keywords_list.xlsx',
        send_result_msg=False
    )

    worker.work(attr=attr, info=info)