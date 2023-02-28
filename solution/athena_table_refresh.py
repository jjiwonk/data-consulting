from worker.abstract_worker import Worker
from utils import athena


class AthenaRefresh(Worker):
    def do_work(self, info:dict, attr:dict):
        database = info['database']
        table_name = info['table_name']

        athena.athena_table_refresh(database=database, table_name=table_name)

        return "Athena Table Refresh Success"


