from solution.DCT649_keyword_monitoring_solution import KeywordMonitoring

if __name__ == "__main__":
    worker = KeywordMonitoring(__file__)
    attr = dict(
        owner_id="29cm", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1yKaWAOY9Se0o66baOJj8t8mezkb9Va-2KOKSAD2aGoM/edit#gid=0",
        keyword_sheet="키워드 설정",
        keyword_column="키워드",
        ad_names='["29cm", "29CM"]',
        slack_channel='dcbot_29cm_네이버sa_키워드순위모니터링'
    )

    worker.work(attr=attr, info=info)
