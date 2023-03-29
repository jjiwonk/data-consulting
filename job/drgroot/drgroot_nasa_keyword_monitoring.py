from solution.DCT820_keyword_monitoring_solution import KeywordMonitoring

if __name__ == "__main__":
    worker = KeywordMonitoring(__file__)
    attr = dict(
        owner_id="drgroot", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        keyword_column="키워드",
        ad_names='{"drgroot": "닥터그루트"}',
        error_slack_channel='ad_닥터그루트_자동입찰_alert',
        file_name='drgroot_keywords_list.csv',
        send_result_msg=False
    )

    worker.work(attr=attr, info=info)
