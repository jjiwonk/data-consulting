from setting import directory as dr
import pandas as pd
import os
import pyarrow as pa
import pyarrow.csv as pacsv
from workers import read_data

#raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/핀다_7팀/8. 요청 및 분석/- 디타겟 분석건_230201/디타겟 분석요청'

class info :
    raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/핀다/DCT576'
    media_file_dir = raw_dir + '/media'

    media_file_sheet_name = {
        '[매체]11월.xlsx': 'AF RAW_1101-1130',
        '[매체]12월.xlsx': 'AF RAW_1201-1231',
        '[매체]1월.xlsx': 'AF RAW_0101-0131'}

    event_raw_dir = raw_dir + '/raw'

def get_media_df(is_concat = True):
    if is_concat == True :
        media_files = os.listdir(info.media_file_dir)

        media_df_list = []
        for file in media_files:
            df = pd.read_excel(info.media_file_dir + '/' + file, sheet_name=info.media_file_sheet_name.get(file))
            media_df_list.append(df)
            del df

        media_df = pd.concat(media_df_list, sort=False, ignore_index=True)
        media_df = media_df.sort_values(['attributed_touch_time', 'advertising_id'])
        media_df = media_df.loc[media_df['event_name'].isin(['re-engagement', 're-attribution'])]
        media_df['Month'] = pd.to_datetime(media_df['attributed_touch_time']).dt.month
        media_df.to_csv(info.raw_dir + '/media_raw.csv', index=False, encoding = 'utf-8-sig')
    else :
        media_df = pd.read_csv(info.raw_dir + '/media_raw.csv')
    return media_df


def get_event_df(user_list):
    event_files = os.listdir(info.event_raw_dir)
    dtypes = {
        'event_time': pa.string(),
        'event_name': pa.string(),
        'media_source': pa.string(),
        'campaign': pa.string(),
        'advertising_id': pa.string()}

    event_df = read_data.pyarrow_csv(dtypes=dtypes, directory=info.event_raw_dir, file_list=event_files)
    event_df = event_df.loc[event_df['advertising_id'].isin(user_list)]
    event_df = event_df.loc[event_df['event_name'].isin(['Opened Finda App', 'loan_contract_completed', 'Viewed LA Home No Result'])]
    event_df = event_df.sort_values('event_time')
    event_df.to_csv(info.raw_dir + '/user_event_raw_data.csv', index=False, encoding = 'utf-8-sig')

    return event_df

def do_work():
    media_df = get_media_df(is_concat=False)
    media_df['touch_id'] = media_df.index
    media_df['click_date'] = pd.to_datetime(media_df['attributed_touch_time']).dt.date

    #unique_user_data = media_df.drop_duplicates(subset = ['Month', 'advertising_id'], keep = 'first')
    unique_user_data = media_df.copy()

    unique_user_data = unique_user_data[['Month', 'advertising_id', 'click_date', 'touch_id']]
    unique_user_data = unique_user_data.sort_values(['advertising_id', 'click_date'])

    user_list = list(unique_user_data['advertising_id'])

    event_df = get_event_df(user_list)
    event_df['event_date'] = event_df['event_time'].apply(lambda x: x[:11])
    event_df['event_date'] = pd.to_datetime(event_df['event_date']).dt.date
    event_df = event_df.sort_values(['advertising_id','event_name', 'event_date'])

    event_df_merge = event_df.merge(unique_user_data, on= 'advertising_id', how = 'left')
    event_df_merge['time_gap'] = event_df_merge['click_date'] - event_df_merge['event_date']
    event_df_merge['time_gap'] = event_df_merge['time_gap'].apply(lambda x : x.days)

    event_df_merge.loc[(event_df_merge['event_name'] == 'Opened Finda App') &
                       (event_df_merge['time_gap']<=13) &
                       (event_df_merge['time_gap']>=1), 'open_14'] = True

    event_df_merge.loc[(event_df_merge['event_name'] == 'loan_contract_completed') &
                       (event_df_merge['time_gap']<=29) &
                       (event_df_merge['time_gap']>=1), 'loan_30'] = True

    event_df_merge.loc[(event_df_merge['event_name'] == 'Viewed LA Home No Result') &
                       (event_df_merge['time_gap']<=29) &
                       (event_df_merge['time_gap']>=1), 'noresult_30'] = True

    event_df_merge.loc[(event_df_merge['event_name'] == 'Viewed LA Home No Result') &
                       (event_df_merge['time_gap']<=59) &
                       (event_df_merge['time_gap']>=1), 'noresult_60'] = True

    event_df_merge = event_df_merge.fillna(False)
    event_df_merge.to_csv(info.raw_dir + '/target_user_event_log_ver3.csv', index=False, encoding='utf-8-sig')

    user_stat_pivot = event_df_merge.pivot_table(index = ['touch_id'], values = ['open_14', 'loan_30', 'noresult_30', 'noresult_60'], aggfunc = 'sum').reset_index()

    for col in ['open_14', 'loan_30', 'noresult_30', 'noresult_60']:
        user_stat_pivot[col] = user_stat_pivot[col].apply(lambda x: True if x > 0 else False)

    media_df_merge = media_df.merge(user_stat_pivot, on =['touch_id'], how = 'left')
    media_df_merge.to_csv(info.raw_dir + '/media_data_event_check_ver3.csv', index=False, encoding = 'utf-8-sig')

    temp = media_df_merge.loc[(media_df_merge['media_source']=='rtb_house')&(media_df_merge['Month']==11)]



# dtypes = {
#     'Event Time': pa.string(),
#     'Event Name': pa.string(),
#     'Media Source': pa.string(),
#     'Campaign': pa.string(),
#     'Advertising ID': pa.string()}
# index_columns = list(dtypes.keys())
# convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
# ro = pacsv.ReadOptions(block_size=10 << 20, encoding='utf-8-sig')
#
# for file in ['[논오가닉]Opened Finda App_2022-11-01_2022-11-30.csv', '[논오가닉]Opened Finda App_2022-12-01_2022-12-31.csv',
#              '[논오가닉]Opened Finda App_2023-01-01_2023-01-31-1.csv', '[논오가닉]Opened Finda App_2023-01-01_2023-01-31-2.csv'] :
#
#     df = pacsv.read_csv(info.event_raw_dir + '/' + file, convert_options=convert_ops, read_options=ro)
#     df = df.to_pandas()
#     df.columns = [col.lower().replace(" ", "_") for col in df.columns]
#
#     df = df.loc[df['event_name'].isin(['Opened Finda App', 'loan_contract_completed', 'Viewed LA Home No Result'])]
#     df.to_csv(info.event_raw_dir + '/' + file, index=False, encoding = 'utf-8-sig')