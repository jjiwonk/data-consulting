from utils import s3
from utils.path_util import get_tmp_path , get_root_directory
from utils import const
from utils import athena
import os
import pandas as pd
from datetime import datetime

from solution.DCT1086_tableau_hyper_upload_tocloud import hyper_file_upload

if __name__ == "__main__":
    worker = hyper_file_upload(__file__)

    def tmp_path_maker(owner_id):
        tmp_path = get_tmp_path() + f"/{owner_id}/"
        os.makedirs(tmp_path, exist_ok=True)
        return tmp_path

    def athena_download(owner_id, query_name):
        # 아데나에서 쿼리를 가져온 후 그 쿼리를 사용하여 데이터 프레임 가져오기
        tmp_path = tmp_path_maker(owner_id)
        s3_query_path = f'query/{owner_id}/{query_name}'
        f_path = s3.download_file(s3_path=s3_query_path, s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET, local_path=tmp_path)

        f = open(f_path, 'r', encoding='utf-8-sig')
        query = str(f.read())
        del f
        df = athena.get_table_data_from_athena('dc_athena', query, 's3')
        os.remove(f_path)
        return df

    def data_prep(owner_id, num_list, text_list , report_col):

        update_df = athena_download('tableau','tableau_keyword_monitoring_update_query.txt')
        update_df = update_df.fillna(0)

        # 기존 데이터와 중복 제거
        tmp_path = tmp_path_maker(owner_id)
        s3_query_path = f'keyword_monitoring/owner_id={owner_id}/update/tableau_keyword_monitoring_hyper.csv'
        f_path = s3.download_file(s3_path=s3_query_path, s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET, local_path=tmp_path)
        last_df = pd.read_csv(f_path)
        os.remove(f_path)

        df = pd.concat([last_df, update_df])
        df['ad_rank'] = df['ad_rank'].fillna(0)
        df = df.fillna('-')

        # data type 정하기
        df[num_list] = df[num_list].astype(int)
        df[text_list] = df[text_list].astype(str)
        df['date'] = pd.to_datetime(df['date'])
        df = df[report_col]

        df = df.drop_duplicates(keep='first')
        df.index = range(len(df))

        # 데이터 s3에 업로드하기
        tmp_path = get_tmp_path() + f"/{owner_id}/"
        file_name = 'tableau_keyword_monitoring_hyper.csv'
        local_path = tmp_path + file_name
        df.to_csv(local_path, encoding='utf-8-sig', index=False)

        year = datetime.now().strftime('%Y')
        month = datetime.now().strftime('%m')
        day = datetime.now().strftime('%d')

        s3.upload_file(local_path,
                       s3_path=f'keyword_monitoring/owner_id={owner_id}/year={year}/month={month}/day={day}/{file_name}',
                       s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET)
        s3.upload_file(local_path,
                       s3_path=f'keyword_monitoring/owner_id={owner_id}/update/{file_name}',
                       s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET)

        os.remove(local_path)

        data = []
        for i in range(len(df)):
            list = df.loc[i].to_list()
            data.append(list)

        return data

    attr = s3.get_info_from_s3('tableau','hyper_upload_solution_tocloud')

    info = dict(
        hyper_name ='keyword_monitoring.hyper',
        project_name = 'RD',
        success_alert_channel = 'pjt_dc_success',
        error_slack_channel='pjt_dc_error',
        num_list = ['ad_rank','year', 'month', 'day', 'hour', 'minute'],
        double_list = [],
        text_list = ['collected_at','pc_mobile_type', 'weekday', 'ad_name','ad_keyword','screenshot_url','url_dict', 'owner_id', 'channel'],
        date_list = ['date']
    )

    info['os_path'] =  get_root_directory() + '/'+ info['hyper_name']
    info['rd_path'] =  get_root_directory() + '/hyperd.log'
    info['report_col'] =  info['num_list'] + info['double_list'] + info['text_list'] + info['date_list']
    info['data'] = data_prep('tableau',info['num_list'], info['text_list'], info['report_col'])

    worker.work(attr=attr, info=info)
