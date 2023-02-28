from solution.athena_table_refresh import AthenaRefresh

if __name__ == "__main__":
    worker = AthenaRefresh(__file__)
    attr = dict(
        owner_id="dc_team",
        product_id= "athena_refresh"
    )

    info = {
        'database' : 'dc_athena',
        'table_name' : 'auto_bid_report',
        'error_slack_channel' : 'pjt_dc_error'
    }

    worker.work(attr=attr, info=info)
