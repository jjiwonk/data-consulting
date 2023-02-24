from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="finda", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1Qk3f7FjPDeOK8hEwAp_PV6TYWjRzf6YclM_1YBqU0p0/edit#gid=0",
        keyword_sheet="키워드 설정(TEST2)",
        keyword_column="키워드",
        ad_names='["finda", "핀다"]',
        customer_id=2503399,
        bid_downgrade=True,
        error_slack_channel='#dc_bot_finda_sa_입찰가솔루션'
    )

    worker.work(attr=attr, info=info)
