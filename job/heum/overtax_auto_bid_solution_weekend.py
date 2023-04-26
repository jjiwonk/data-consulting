from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="heum", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1WQac1GBFmitFLch28Ce0EBokvS9lrJI9u__mfLGI36Q/edit#gid=0",
        keyword_sheet="더낸세금_주말",
        keyword_column="키워드",
        ad_names='["heumtax"]',
        customer_id= 1281678,
        bid_downgrade=True,
        error_slack_channel='heum_sa_입찰가솔루션'
    )

    worker.work(attr=attr, info=info)
