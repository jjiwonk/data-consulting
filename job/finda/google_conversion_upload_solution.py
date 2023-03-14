from solution.gclid_upload_solution import clickconversion_upload
import pandas as pd
import os
from utils.path_util import get_tmp_path
from utils import s3
from utils import const
import datetime
from utils import athena

if __name__ == "__main__":
    worker = clickconversion_upload(__file__)

    # 아데나에서 쿼리를 가져온 후 그 쿼리를 사용하여 데이터 프레임 가져오기
    def athena_download(owner_id):
        tmp_path = get_tmp_path() + f"/{owner_id}/"

        os.makedirs(tmp_path, exist_ok=True)

        s3_path = f'query/{owner_id}/gclid_query.txt'
        f_path = s3.download_file(s3_path=s3_path, s3_bucket=const.DEFAULT_S3_PRIVATE_BUCKET, local_path=tmp_path)

        f = open(f_path, 'r', encoding='utf-8-sig')
        query = str(f.read())

        del f
        df = athena.get_table_data_from_athena('dmp_athena', query)
        os.remove(f_path)
        return df

    def data_prep():
        df = athena_download('finda')

        # 기여기간 가공
        df = df.sort_values(by=['event_time'], ascending=True)

        df['event_time'] = pd.to_datetime(df['event_time'])
        df['install_time'] = pd.to_datetime(df['install_time'])
        df['attributed_touch_time'] = pd.to_datetime(df['attributed_touch_time'])

        df['CTET'] = df['event_time'] - df['attributed_touch_time']
        df['CTIT'] = df['install_time'] - df['attributed_touch_time']

        df['conversion_action_id'] = '-'

        def conversion_id(df, event_name, platform, conversion_id):
            if event_name == 'install':
                df.loc[(df['event_name'].str.contains(event_name)) & (df['platform'] == platform) & (
                        df['CTIT'] < datetime.timedelta(days=1)), 'conversion_action_id'] = conversion_id
            else:
                df.loc[(df['event_name'].str.contains(event_name)) & (df['platform'] == platform) & (
                        df['CTET'] < datetime.timedelta(days=1)), 'conversion_action_id'] = conversion_id
            return df

        # conversion_id 값 추가 될때 별도 쿼리로 id 확인 필요
        df = conversion_id(df, 'loan_contract_completed', 'android', '6469848862')
        df = conversion_id(df, 'loan_contract_completed', 'ios', '6469884462')
        df = conversion_id(df, 'Viewed LA Home', 'android', '6469870061')
        df = conversion_id(df, 'Viewed LA Home', 'ios', '6469880541')
        df = conversion_id(df, 'Clicked Signup', 'android', '6469868867')
        df = conversion_id(df, 'Clicked Signup', 'ios', '6469873439')
        df = conversion_id(df, 'install', 'android', '6469097954')
        df = conversion_id(df, 'install', 'android', '6469872884')

        df = df.loc[df['conversion_action_id'] != '-']

        # 한도 조회 유저 중복제거
        limit_df = df.loc[df['event_name'].str.contains('Viewed LA Home')]
        limit_df = limit_df.drop_duplicates(['appsflyer_id'], keep='first')
        df = df.loc[~df['event_name'].str.contains('Viewed LA Home')]
        df = pd.concat([df, limit_df])

        # API 양식에 맞게 가공
        prep_df = df[['sub_param_1', 'sub_param_2', 'sub_param_3', 'conversion_action_id',
                      'attributed_touch_time']].drop_duplicates()
        prep_df['attributed_touch_time'] = prep_df['attributed_touch_time'].apply(
            lambda x: x.strftime('%Y-%m-%d %H:%M:%S+00:00'))
        prep_df = prep_df.rename(columns={'sub_param_1': 'gclid', 'sub_param_2': 'gbraid', 'sub_param_3': 'wbraid',
                                          'attributed_touch_time': 'conversion_date_time'})
        prep_df.index = range(len(prep_df))

        return prep_df

    data = data_prep()

    attr = dict(
        owner_id="finda"
    )
    info = dict(
        customer_id ='4983132762',
        success_alert_channel = 'pjt_dc_success',
        error_slack_channel='ad_finda_sa_실무방',
        data = data
    )

    worker.work(attr=attr, info=info)