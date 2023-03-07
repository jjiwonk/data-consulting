from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="siv", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1JCzrSfP571_G6wR7jcFzIGQ_nt7Gz3DatkUuCS2cmb8/edit#gid=1112513230",
        keyword_sheet="키워드 설정_주말",
        keyword_column="키워드",
        ad_names='["sivillage", "시마을"]',
        customer_id=1084895,
        bid_downgrade=True,
        error_slack_channel='ad_시마을_자동입찰_alert'
    )

    worker.work(attr=attr, info=info)
