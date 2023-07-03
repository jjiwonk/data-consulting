from solution.DCT697_auto_bid_solution import AutoBidSolution

if __name__ == "__main__":
    worker = AutoBidSolution(__file__)
    attr = dict(
        owner_id="mimacstudy", channel="네이버SA"
    )
    info = dict(
        media_info='네이버SA',
        use_headless=True,
        spread_sheet_url="https://docs.google.com/spreadsheets/d/1OKz6_SOtJiKmW02_e-QLf9R4xQOLAMYaQsvbihfsREQ/edit#gid=0",
        keyword_sheet="키워드 설정(주간 22~23시)",
        keyword_column="키워드",
        ad_names='["대성마이맥", "mimacstudy"]',
        customer_id=150839,
        bid_downgrade=True,
        error_slack_channel='ad_대성마이맥_자동입찰_모니터링'
    )

    worker.work(attr=attr, info=info)
