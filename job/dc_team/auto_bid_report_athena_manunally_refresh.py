from solution.athena_table_manually_refresh import AthenaRefresh

if __name__ == "__main__":
    worker = AthenaRefresh(__file__)
    attr = dict(
        owner_id="dc_team",
        product_id="athena_refresh"
    )

    info = dict(
        database="dc_athena",
        table_name="auto_bid_report",
        table_s3_path="data-consulting-private/auto_bid/",
        owner_ids=['heum', 'kolonmall', 'samsungkracc'],
        channel='네이버SA',
        start_date='2023-07-01',
    )

    worker.work(attr=attr, info=info)
