from utils import athena
from utils.dropbox_util import upload_v2
from worker.abstract_worker import Worker
import os


class AthenaToDropbox(Worker):
    def __init__(self, job_name = None):
        super().__init__(job_name)

    def do_work(self, info: dict, attr: dict):
        file_name = info['file_name']
        dropbox_path = info['dropbox_path']
        query = info['query']
        source = info['source']
        try:
            df = athena.fetchall_athena('dmp_athena', query, source)
            df.to_csv(file_name, index=False, encoding='utf-8')
            upload_v2(file_name, dropbox_path + '/' + file_name)
            os.remove(file_name)
            return 'AthenaToDropbox Job complete.'
        except Exception as e:
            raise e
