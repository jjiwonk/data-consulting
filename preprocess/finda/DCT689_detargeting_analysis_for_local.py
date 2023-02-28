from setting import directory as dr
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import datetime


class info :
    #raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/2. 업무/RE_디타겟점검'
    raw_dir = dr.download_dir + '/RE_디타겟점검'
    #today = datetime.date.today()
    today = datetime.date(2023, 3, 1)
    yearmonth = today.strftime('%Y%m')

    media_file_name = f'[매체]인스톨_당월_{yearmonth}.csv'

    re_event_now_file_name = f'[논오가닉]RE이벤트_당월_{yearmonth}.csv'
    re_event_past_file_name1 = f'[논오가닉]RE이벤트_전월_{yearmonth}.csv'
    re_event_past_file_name2 = f'[논오가닉]RE이벤트_전전월_{yearmonth}.csv'

    ua_event_now_file_name = f'[논오가닉]UA이벤트_당월_{yearmonth}.csv'
    ua_event_past_file_name1 = f'[논오가닉]UA이벤트_전월_{yearmonth}.csv'
    ua_event_past_file_name2 = f'[논오가닉]UA이벤트_전전월_{yearmonth}.csv'

    organic_event_now_file_name = f'[오가닉]이벤트_당월_{yearmonth}.csv'
    organic_event_past_file_name1 = f'[오가닉]이벤트_전월_{yearmonth}.csv'
    organic_event_past_file_name2 = f'[오가닉]이벤트_전전월_{yearmonth}.csv'

    event_file_list = [re_event_now_file_name, re_event_past_file_name1, re_event_past_file_name2,
                       ua_event_now_file_name, ua_event_past_file_name1, ua_event_past_file_name2,
                       organic_event_now_file_name, organic_event_past_file_name1, organic_event_past_file_name2]

    credit_file_name = f'신용점수_{yearmonth}.csv'

    result_file_name = f'media_data_event_check_{yearmonth}.csv'

class prep :
    def pyarrow_csv(self, dtypes, directory, file_list, encoding='utf-8-sig'):
        index_columns = list(dtypes.keys())
        convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
        ro = pacsv.ReadOptions(block_size=10 << 20, encoding=encoding)
        table_list = []

        for file in file_list:
            temp = pacsv.read_csv(directory + '/' + file, convert_options=convert_ops, read_options=ro)
            table_list.append(temp)
            print(file + ' Read 완료.')

        raw_data = pa.concat_tables(table_list)
        raw_data = raw_data.to_pandas()

        return raw_data

    def get_media_df(self):
        dtypes = {
            'attributed_touch_time': pa.string(),
            'install_time': pa.string(),
            'event_time': pa.string(),
            'event_name': pa.string(),
            'media_source': pa.string(),
            'campaign': pa.string(),
            'advertising_id': pa.string()}

        media_df = self.pyarrow_csv(dtypes, info.raw_dir, [info.media_file_name])
        media_df = media_df.sort_values(['attributed_touch_time', 'advertising_id'])
        media_df = media_df.loc[media_df['event_name'].isin(['re-engagement', 're-attribution'])]
        media_df['Month'] = pd.to_datetime(media_df['attributed_touch_time']).dt.month
        media_df['touch_id'] = media_df.index
        media_df['click_date'] = pd.to_datetime(media_df['attributed_touch_time']).dt.date
        return media_df

    def get_event_df(self, user_list):
        dtypes = {
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'Media Source': pa.string(),
            'Campaign': pa.string(),
            'Advertising ID': pa.string()}

        event_df = self.pyarrow_csv(dtypes=dtypes, directory=info.raw_dir, file_list=info.event_file_list)
        event_df.columns = [col.lower().replace(' ', '_') for col in event_df.columns]
        event_df = event_df.loc[event_df['advertising_id'].isin(user_list)]
        event_df = event_df.loc[event_df['event_name'].isin(['Opened Finda App', 'loan_contract_completed', 'Viewed LA Home No Result'])]
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
                           (event_df_merge['time_gap'] >= 1), 'open_14'] = True

        event_df_merge.loc[(event_df_merge['event_name'] == 'loan_contract_completed') &
                           (event_df_merge['time_gap'] <= 29) &
                           (event_df_merge['time_gap'] >= 1), 'loan_30'] = True

        event_df_merge.loc[(event_df_merge['event_name'] == 'Viewed LA Home No Result') &
                           (event_df_merge['time_gap'] <= 59) &
                           (event_df_merge['time_gap'] >= 1), 'noresult_60'] = True

        # True 이외의 값은 모두 False로 채움
        event_df_merge = event_df_merge.fillna(False)

        user_stat_pivot = event_df_merge.pivot_table(index=['touch_id'],
                                                        values=['open_14', 'loan_30', 'noresult_60'],
                                                        aggfunc='sum').reset_index()

        for col in ['open_14', 'loan_30', 'noresult_60']:
            user_stat_pivot[col] = user_stat_pivot[col].apply(lambda x: True if x > 0 else False)
        return user_stat_pivot

class rd :
    def __init__(self):
        self.media_df = prep().get_media_df()
        self.user_list = list(self.media_df['advertising_id'].unique())
        self.event_df = prep().get_event_df(user_list=self.user_list)
        self.unique_user_data = prep().get_unique_user_data(df=self.media_df)
        self.user_stat_pivot = prep().get_user_stat_pivot(event_df = self.event_df, unique_user_data=self.unique_user_data)


rd = rd()

# 매체 데이터와 유저 특성 데이터 병합
media_df_merge = rd.media_df.merge(rd.user_stat_pivot, on =['touch_id'], how = 'left')

credit_file = pd.read_csv(info.raw_dir + '/' + info.credit_file_name)
credit_file['credit'] = True

media_df_merge_final = media_df_merge.merge(credit_file, on = 'advertising_id', how = 'left')
media_df_merge_final['credit'] = media_df_merge_final['credit'].fillna(False)

media_df_merge_final.loc[media_df_merge_final[['open_14', 'loan_30',  'noresult_60','credit']].values.sum(axis=1) > 0, 'result'] = True
media_df_merge_final['result'] = media_df_merge_final['result'].fillna(False)

media_df_merge_final.to_csv(info.raw_dir + '/' + info.result_file_name, index=False, encoding = 'utf-8-sig')