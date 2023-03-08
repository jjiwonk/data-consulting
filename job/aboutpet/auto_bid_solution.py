from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="aboutpet", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1Ko6eKl1DwLf9WC4plMAQCcp3kiThiXtyVKTPyumr-0A/edit#gid=1786902477",
        keyword_sheet="키워드 설정",
        keyword_column="키워드",
        ad_names='["aboutpet", "어바웃펫"]',
        customer_id=708273,
        bid_downgrade=True,
        error_slack_channel='ad_어바웃펫_자동입찰_alert'
    )

    worker.work(attr=attr, info=info)
