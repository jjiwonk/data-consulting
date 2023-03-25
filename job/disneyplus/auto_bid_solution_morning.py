from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="disneyplus", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1pQg63dqpCGBcQAi4wTaqr_2xV4OWcnBEbDUCj4TZ7cE/edit#gid=2116326160",
        keyword_sheet="일반_매일 00시~12시",
        keyword_column="키워드",
        ad_names='["디즈니플러스", "disneyplus"]',
        customer_id=2384006,
        bid_downgrade=True,
        error_slack_channel='gl_글로벌_디즈니플러스'
    )

    worker.work(attr=attr, info=info)
