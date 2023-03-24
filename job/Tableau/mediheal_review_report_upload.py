from utils import s3
from utils.path_util import get_tmp_path
from utils import const
from utils import os_util
import os
import pandas as pd
from datetime import datetime

from solution.DCT873_tableau_hyper_upload import hyper_file_upload

if __name__ == "__main__":
    worker = hyper_file_upload(__file__)

    def tmp_path_maker(owner_id):
        tmp_path = get_tmp_path() + f"/{owner_id}/"
        os.makedirs(tmp_path, exist_ok=True)

        if os_util.is_windows_os():
            tmp_path = tmp_path.replace('/', '\\')
        return tmp_path

    def s3_download(owner_id):
        tmp_path = tmp_path_maker(owner_id)
        yearmonth = datetime.now().strftime('%Y%m')
        s3_path = f'review_data_report/owner_id={owner_id}/' + f'{owner_id}_Review_Data_Report_{yearmonth}.csv'
        f_path = s3.download_file(s3_path=s3_path, s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET, local_path=tmp_path)
        df = pd.read_csv(f_path)
        os.remove(f_path)
        return df

    def data_prep(owner_id, num_list, text_list, date_list):
        df = s3_download(owner_id)
        df = df.fillna(0)

        # data type 정하기
        df[num_list] = df[num_list].astype(int)
        df[text_list] = df[text_list].astype(str)
        df[date_list] = df[date_list].apply(lambda x : pd.to_datetime(x))
        df = df[num_list + text_list + date_list]

        df = df.drop_duplicates(keep='first')
        df.index = range(len(df))

        data = []
        for i in range(len(df)):
            list = df.loc[i].to_list()
            data.append(list)

        return data

    attr = s3.get_info_from_s3('tableau','hyper_upload_solution')
    attr['owner_id'] = 'mediheal'

    info = dict(
        hyper_name ='mediheal_review_report.hyper',
        project_name = 'RD',
        success_alert_channel = 'gl_메디힐_모니터링_alert',
        error_slack_channel='gl_메디힐_모니터링_alert',
        num_list = ['rating', 'count', 'review_length'],
        text_list = ['사이트 구분', '제조사 구분', '제조사', '제품명', 'user_name',
                     'review_text', 'review_id', 'sentiment', 'review_text_prep',
                     'nouns_list', 'keyword'],
        date_list = ['review_date']
    )

    info['data'] = data_prep(attr['owner_id'], info['num_list'], info['text_list'], info['date_list'])

    worker.work(attr=attr, info=info)
