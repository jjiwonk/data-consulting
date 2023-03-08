from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="drgroot", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1NCSCSOKMDN624H7M-M3Rhf9Rw90CWiJNgPnh_Mmg_OM/edit#gid=0",
        keyword_sheet="키워드 설정(주말)",
        keyword_column="키워드",
        ad_names='["닥터그루트", "drgroot"]',
        customer_id= 1147019,
        bid_downgrade=True,
        error_slack_channel='ad_닥터그루트_자동입찰_alert'
    )

    worker.work(attr=attr, info=info)
