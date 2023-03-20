from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="kolonmall", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1d36Wd5hhl1tKvYy-iwVQc7g2NPME-w4giYsCJa7Yi7M/edit#gid=0",
        keyword_sheet="키워드 설정",
        keyword_column="키워드",
        ad_names='["코오롱몰", "kolonmall"]',
        customer_id=533124,
        bid_downgrade=True,
        error_slack_channel='코오롱몰_sa자동입찰'
    )

    worker.work(attr=attr, info=info)
