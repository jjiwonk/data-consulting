from solution.athena_table_manually_refresh import AthenaRefresh
from datetime import datetime

if __name__ == "__main__":
    worker = AthenaRefresh(__file__)
    attr = dict(
        owner_id="dc_team",
        product_id="athena_refresh"
    )

    info = dict(
        database="dc_athena",
        table_name="keyword_monitoring_report",
        table_s3_path="data-consulting-private/keyword_monitoring/",
        owner_ids=['heum', 'disneyplus', 'drgroot'],
        channel='네이버SA',
        start_date=datetime.now().replace(day=1).strftime("%Y-%m-%d"),
    )

    worker.work(attr=attr, info=info)
