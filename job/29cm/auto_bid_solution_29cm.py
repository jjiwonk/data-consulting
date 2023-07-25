from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="29cm", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1FYsv2l6I0_Hh55n0DLIjmcgSHZ2pVni6r7DkuSept7M/edit#gid=0",
        keyword_sheet="키워드 설정(29cm)",
        keyword_column="키워드",
        ad_names='["29cm.co.kr", "29CM"]',
        customer_id= 412694,
        bid_downgrade=True,
        error_slack_channel='ad_29cm_자동입찰_alert'
    )

    worker.work(attr=attr, info=info)