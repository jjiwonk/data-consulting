from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="thehandsome", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1-Cg2f7wQ6bSSBKhAWevhLuOSETRDfRxJQzkTDEMKZK4/edit#gid=0",
        keyword_sheet="키워드 설정_메인키워드",
        keyword_column="키워드",
        ad_names='["thehandsome"]',
        customer_id=491016,
        bid_downgrade=True,
        error_slack_channel='ad_더한섬닷컴_sa자동입찰'
    )

    worker.work(attr=attr, info=info)
