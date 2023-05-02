from setting import directory as dr
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import datetime
from datetime import date, timedelta
import os

class info :
    prizm_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 리포트/자동화리포트/appsflyer_prism_2'
    raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/RAW'
    result_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검/RAW_FIN'

    # 돌리고자 하는 데이터의 당월 기준을 적어주세요! (ex. 3,4 월 데이터 뽑고 싶으면 5 기재)
    month = 5

    today = datetime.date(2023, month , 1)
    yearmonth = today.strftime('%Y%m')
    one_month_ago = today.replace(day=1) - timedelta(days=1)
    two_month_ago = one_month_ago.replace(day=1) - timedelta(days=1)
    one_yearmonth = one_month_ago.strftime('%Y%m')
    two_yearmonth = two_month_ago.strftime('%Y%m')

    target_event_list = ['Opened Finda App', 'loan_contract_completed', 'Viewed LA Home No Result']
    loan_event = ['Viewed LA Home No Result', 'loan_contract_completed']
    re_media = ['appier_int', 'cauly_int', 'remerge_int', 'rtbhouse_int', 'kakao_int', 'googleadwords_int']

    #오가닉
    organic_event_file_list = [f'[오가닉]이벤트1-1_전전월_{yearmonth}.csv', f'[오가닉]이벤트1-2_전전월_{yearmonth}.csv',
                              f'[오가닉]이벤트1-1_전월_{yearmonth}.csv', f'[오가닉]이벤트1-2_전월_{yearmonth}.csv']

    # 신용 점수 데이터
    credit_file_name = f'신용점수_{yearmonth}.csv'

    # 앱푸시 데이터
    apppush_file_name = f'앱푸시마수동_{yearmonth}.csv'

    # 클릭 유저 데이터
    clickuser_file_name = f'클릭유저_{yearmonth}.csv'

    # 최종 결과 파일명
    result_file_name = f'media_data_event_check_{yearmonth}.csv'

class prep :
    def pyarrow_csv(self, dtypes, directory, file_list, encoding='utf-8-sig'):
        index_columns = list(dtypes.keys())
        convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
        ro = pacsv.ReadOptions(block_size=10 << 20, encoding=encoding)
        table_list = []

        for file in file_list:
            try :
                temp = pacsv.read_csv(directory + '/' + file, convert_options=convert_ops, read_options=ro)
                table_list.append(temp)
                print(file + ' Read 완료.')
            except :
                print(file + '을 불러오는데 실패하였습니다.')

        raw_data = pa.concat_tables(table_list)
        raw_data = raw_data.to_pandas()

        return raw_data

    def raw_file_read(self):

        raw_files = os.listdir(info.prizm_dir)
        prep_files = []

        for i in range(len(raw_files)):
            if raw_files[i].find(info.one_yearmonth) != -1:
                prep_files.append(raw_files[i])
            elif raw_files[i].find(info.two_yearmonth) != -1:
                prep_files.append(raw_files[i])
            else:
                pass

        dtypes = {
            'attributed_touch_time': pa.string(),
            'attributed_touch_type': pa.string(),
            'install_time': pa.string(),
            'event_time': pa.string(),
            'event_name': pa.string(),
            'media_source': pa.string(),
            'is_retargeting': pa.string(),
            'campaign': pa.string(),
            'advertising_id': pa.string()}

        raw_data = self.pyarrow_csv(dtypes=dtypes, directory=info.prizm_dir, file_list=prep_files)

        return raw_data

    def data_prep(self , raw , file_name):

        df = raw.loc[raw['attributed_touch_type'] == 'click']
        df['event_time'] = pd.to_datetime(df['event_time'])
        df['year_month'] = df['event_time'].apply(lambda x: x.strftime('%Y%m'))

        if file_name == 'conversion_data':
            # conversion_data 정의
            conversion_data = df.loc[df['year_month'] == info.one_yearmonth]
            conversion_data = conversion_data.loc[conversion_data['event_name'].isin(['re-engagement', 're-attribution'])]
            conversion_data = conversion_data.loc[conversion_data['is_retargeting'] == 'True']
            conversion_data = conversion_data.loc[conversion_data['media_source'].isin(info.re_media)]

            conversion_data['구분'] = conversion_data['campaign'].apply(
                lambda x: x.replace(x, 'NU') if x.find('NU_') != -1 else '-')
            conversion_data = conversion_data.loc[conversion_data['구분'] != 'NU']
            df = conversion_data[['attributed_touch_time','install_time','event_time','event_name','media_source','campaign','advertising_id']]
            df.to_csv(info.result_dir + f'/conversion_data_{info.yearmonth}.csv', encoding = 'utf-8-sig', index = False)

        elif file_name == 'appopen_data':
            df = df.loc[df['event_name'] == 'Opened Finda App']
            df = df[['event_time','event_name','media_source','campaign','advertising_id']]
            df.index = range(len(df))
            df1 = df[0:1000000]
            df2 = df[1000000:]
            df1.to_csv(info.result_dir + f'/appopen_data_{info.yearmonth}_1.csv', encoding='utf-8-sig', index=False)
            df2.to_csv(info.result_dir + f'/appopen_data_{info.yearmonth}_2.csv', encoding='utf-8-sig', index=False)

        else:
            df = df.loc[df['event_name'].isin(['Viewed LA Home No Result', 'loan_contract_completed'])]
            df = df[['event_time', 'event_name', 'media_source', 'campaign', 'advertising_id']]
            df.to_csv(info.result_dir + f'/loan_data_{info.yearmonth}.csv', encoding='utf-8-sig',index=False)

        return df

    def get_media_df(self , media_raw):
        media_df = media_raw
        media_df = media_df.sort_values(['attributed_touch_time', 'advertising_id'])
        media_df['Month'] = pd.to_datetime(media_df['attributed_touch_time']).dt.month
        media_df['touch_id'] = media_df.index
        media_df['click_date'] = pd.to_datetime(media_df['attributed_touch_time']).dt.date
        return media_df

    def get_event_df(self, raw , user_list, data_type):
        if data_type == 'organic' :
            dtypes = {
                'Event Time': pa.string(),
                'Event Name': pa.string(),
                'Media Source': pa.string(),
                'Campaign': pa.string(),
                'Advertising ID': pa.string()}
            file_list = info.organic_event_file_list
            event_df = self.pyarrow_csv(dtypes=dtypes, directory=info.raw_dir, file_list=file_list)

        elif data_type == 'prism' :
            appopen_df = self.data_prep(raw,'appopen_data')
            loan_df = self.data_prep(raw,'loan')
            event_df = pd.concat([appopen_df,loan_df])

        if data_type == 'organic' :
            event_df.columns = [col.lower().replace(' ', '_') for col in event_df.columns]

        event_df = event_df.loc[event_df['advertising_id'].isin(user_list)]
        event_df = event_df.loc[event_df['event_name'].isin(info.target_event_list)]
        event_df = event_df.sort_values('event_time')
        event_df['event_date'] = pd.to_datetime(event_df['event_time']).dt.date
        event_df = event_df.sort_values(['advertising_id', 'event_name', 'event_date'])
        return event_df

    def get_unique_user_data(self, df):
        unique_user_data = df
        unique_user_data = unique_user_data[['Month', 'advertising_id', 'click_date', 'touch_id']]
        unique_user_data = unique_user_data.sort_values(['advertising_id', 'click_date'])
        return unique_user_data

    def get_user_stat_pivot(self, event_df, unique_user_data):
        # 이벤트 데이터와 유니크 유저 데이터를 adid 기준으로 병합
        event_df_merge = event_df.merge(unique_user_data, on='advertising_id', how='left')

        # 클릭 시간부터 이벤트 발생시간 까지의 시차 계산
        event_df_merge['time_gap'] = event_df_merge['click_date'] - event_df_merge['event_date']
        event_df_merge['time_gap'] = event_df_merge['time_gap'].apply(lambda x: x.days)

        # 시차를 기준으로 14일 이내, 30일 이내, 60일 이내 판별
        event_df_merge.loc[(event_df_merge['event_name'] == 'Opened Finda App') &
                           (event_df_merge['time_gap'] <= 13) &
                           (event_df_merge['time_gap'] >= 1), 'open_14'] = 1

        event_df_merge.loc[(event_df_merge['event_name'] == 'loan_contract_completed') &
                           (event_df_merge['time_gap'] <= 29) &
                           (event_df_merge['time_gap'] >= 1), 'loan_30'] = 1

        event_df_merge.loc[(event_df_merge['event_name'] == 'Viewed LA Home No Result') &
                           (event_df_merge['time_gap'] <= 59) &
                           (event_df_merge['time_gap'] >= 1), 'noresult_60'] = 1

        # True 이외의 값은 모두 False로 채움
        event_df_merge = event_df_merge.fillna(0)

        user_stat_pivot = event_df_merge.pivot_table(index=['touch_id'],
                                                        values=['open_14', 'loan_30', 'noresult_60'],
                                                        aggfunc='sum').reset_index()

        for col in ['open_14', 'loan_30', 'noresult_60']:
            user_stat_pivot[col] = user_stat_pivot[col].apply(lambda x: 1 if x > 0 else 0)
        return user_stat_pivot

class rd :
    def __init__(self):
        self.raw = prep().raw_file_read()
        self.media_raw = prep().data_prep(self.raw ,'conversion_data')
        self.media_df = prep().get_media_df(self.media_raw)
        self.user_list = list(self.media_df['advertising_id'].unique())
        self.event_df = pd.concat([prep().get_event_df(self.raw ,user_list=self.user_list, data_type='prism'),
                                   prep().get_event_df(self.raw ,user_list=self.user_list, data_type='organic')], ignore_index=True)
        self.unique_user_data = prep().get_unique_user_data(df=self.media_df)
        self.user_stat_pivot = prep().get_user_stat_pivot(event_df = self.event_df, unique_user_data=self.unique_user_data)


rd = rd()

# 매체 데이터와 유저 특성 데이터 병합

media_df_merge = rd.media_df.merge(rd.user_stat_pivot, on =['touch_id'], how = 'left')

credit_file = pd.read_csv(info.raw_dir + '/' + info.credit_file_name).drop_duplicates()
credit_file['credit'] = 1

apppush_file = pd.read_csv(info.raw_dir + '/' + info.apppush_file_name).drop_duplicates()
apppush_file['agree&mkt'] = 1

clickuser_file = pd.read_csv(info.raw_dir + '/' + info.clickuser_file_name).drop_duplicates()
clickuser_file['click'] = 1

media_df_merge_final = media_df_merge.merge(credit_file, on = 'advertising_id', how = 'left')
media_df_merge_final['credit'] = media_df_merge_final['credit'].fillna(0)

media_df_merge_final = media_df_merge_final.merge(apppush_file, on = 'advertising_id', how = 'left')
media_df_merge_final['agree&mkt'] = media_df_merge_final['agree&mkt'].fillna(0)

media_df_merge_final = media_df_merge_final.merge(clickuser_file, on = 'advertising_id', how = 'left')
media_df_merge_final['click'] = media_df_merge_final['click'].fillna(0)

media_df_merge_final[['open_14', 'loan_30',  'noresult_60']] = media_df_merge_final[['open_14', 'loan_30',  'noresult_60']].fillna(0)

media_df_merge_final.loc[media_df_merge_final[['open_14', 'loan_30',  'noresult_60','credit','agree&mkt','click']].values.sum(axis=1) > 0, 'result'] = 1
media_df_merge_final['result'] = media_df_merge_final['result'].fillna(0)

media_df_merge_final.to_csv(info.result_dir + '/' + info.result_file_name, index=False, encoding = 'utf-8-sig')

