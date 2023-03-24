from solution.DCT878_gdrive_upload import GdriveUpload
import os
from utils.path_util import get_tmp_path
from utils import athena
from utils import s3
from utils import const
from utils import google_drive
from datetime import datetime

class raw_data :
    gdrive = google_drive.GoogleDrive()

    def athena_download(self, owner_id, query_file_name):
        tmp_path = get_tmp_path() + f"/{owner_id}/"

        os.makedirs(tmp_path, exist_ok=True)

        s3_path = f'query/{owner_id}/{query_file_name}'
        f_path = s3.download_file(s3_path=s3_path, s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET, local_path=tmp_path)

        f = open(f_path, 'r', encoding='utf-8-sig')
        query = str(f.read())

        del f
        df = athena.get_table_data_from_athena('dmp_athena', query)
        os.remove(f_path)
        return df

    def get_index_sheet(self, sheet_name):
        sheet_url = 'https://docs.google.com/spreadsheets/d/1nN-A0_f1ZpeUSS5JEibGcKKkoTgdgibGStTt_qZkdh0/edit#gid=0'
        index_sheet = self.gdrive.get_work_sheet(url = sheet_url, sheet_name = sheet_name)
        index_df = self.gdrive.sheet_to_df(sheet = index_sheet)

        return index_df

    def prep(self, df, index_df, keyword_index_df):
        index_df = index_df.drop_duplicates('campaign_name', keep = 'last')
        merge_df = df.merge(index_df, on = 'campaign_name', how = 'left')
        merge_df = merge_df.rename(columns = {'media_source' : 'data_source'})

        keyword_index_df = keyword_index_df.drop_duplicates('ad_keyword', keep='last')
        merge_df = merge_df.merge(keyword_index_df, on = 'ad_keyword', how='left')
        return merge_df

if __name__ == "__main__":
    worker = GdriveUpload(__file__)

    df = raw_data().athena_download('finda','keyword_spend_data_query.txt')
    index_df = raw_data().get_index_sheet('INDEX')
    keyword_index_df = raw_data().get_index_sheet('그룹 INDEX')
    merge_df = raw_data().prep(df, index_df, keyword_index_df)

    attr = dict(
        owner_id="finda", schedule_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )
    info = dict(
        df = merge_df,
        folder_id = '1tkaPtYAlRt0Zq3P4rQ6u4JCY7pN-7Xzz',
        file_name = 'finda_keyword_cost_data',
        file_format = 'csv',
        slack_channel='ad_finda_sa_실무방',
        error_slack_channel='ad_finda_sa_실무방'
    )

    worker.work(attr=attr, info=info)
