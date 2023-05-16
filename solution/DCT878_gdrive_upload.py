import os

from worker.abstract_worker import Worker
from utils import google_drive
from utils.path_util import get_tmp_path

from datetime import datetime

class GdriveUpload(Worker):
    def do_work(self, info:dict, attr:dict):
        owner_id = attr['owner_id']
        schedule_time = attr['schedule_time']

        df = info['df']
        folder_id = info['folder_id']
        upload_file_name = info['file_name']
        file_format = info['file_format']

        tmp_path = get_tmp_path() + "/" + owner_id + "/"

        schedule_date = datetime.strptime(schedule_time, '%Y-%m-%d %H:%M:%S')
        time_str = schedule_date.strftime('%Y-%m-%d')
        file_name = f'{upload_file_name}_{time_str}.{file_format}'

        local_path = tmp_path + file_name
        if file_format == 'csv' :
            df.to_csv(local_path, index = False, encoding = 'utf-8-sig')
        else :
            print('지원하지 않는 파일 포맷입니다')
            raise Exception
        google_drive.GoogleDriveClient().file_upload(file_paths = local_path, name = file_name, folder_id = folder_id)
        os.remove(local_path)

        return "Gdrive Upload Success"