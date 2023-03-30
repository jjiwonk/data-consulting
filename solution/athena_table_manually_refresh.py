from worker.abstract_worker import Worker
from utils import athena


class AthenaRefresh(Worker):
    def do_work(self, info: dict, attr: dict):
        database = info['database']
        table_name = info['table_name']
        table_s3_path = info['table_s3_path']  # 파티션 바로 앞까지 s3 path
        owner_ids = info['owner_ids']
        channel = info['channel']
        start_date = info['start_date']

        for owner_id in owner_ids:
            athena.athena_table_manually_refresh(database, table_name, table_s3_path, owner_id, channel, start_date)

        return "Athena Table Refresh Success"
