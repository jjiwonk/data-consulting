from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="samsungkracc", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1k4uQxx6n0k1Tv3IB34MQc6i35KAoE6VPzG5Fc1UEMN4/edit#gid=1616671540",
        keyword_sheet="자동입찰_네이버SA_키워드 설정",
        keyword_column="키워드",
        ad_names='["samsungpop", "삼성증권"]',
        customer_id=2007160,
        bid_downgrade=True,
        error_slack_channel='alert_삼성증권_nsa자동입찰'
    )

    worker.work(attr=attr, info=info)
