import datetime
import pandas as pd
from setting import directory as dr
from workers import read_data
import os
import pyarrow as pa
from workers.func import user_identifier

def read_organic():
    def read_file(OS):
        file_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_organic/{OS}'
        file_list = os.listdir(file_dir)
        files = [f for f in file_list if 'in-app-events' in f]

        dtypes = {
            'Install Time': pa.string(),
            'Event Time': pa.string(),
            'Event Name': pa.string(),
            'AppsFlyer ID': pa.string(),
            'Customer User ID': pa.string()}

        data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=files)
        return data

    ios = read_file('ios')
    aos = read_file('aos')

    organic_data = pd.concat([ios,aos])

    organic_data['Event Time'] = pd.to_datetime(organic_data['Event Time'])
    organic_data = organic_data.loc[organic_data['Event Time'] >= datetime.datetime(year=2022, month=7, day=1)]

    return organic_data

organic_data = read_organic()
organic_data.columns = [col.lower().replace(' ','_') for col in organic_data.columns]

def read_paid():

    file_dir = dr.dropbox_dir +  f'/광고사업부/4. 광고주/핀다_7팀/14. AF RAW 합본/- raw_af data (prism)'
    file_list = os.listdir(file_dir)
    file_list = [file for file in file_list if '.csv' in file]

    dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'event_name': pa.string(),
        'appsflyer_id': pa.string(),
        'media_source' : pa.string(),
        'advertising_id' : pa.string(),
        'idfa' : pa.string()}

    data = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)
    data = data.sort_values('event_time')

    data['event_time'] = pd.to_datetime(data['event_time'])
    paid_data = data.loc[(data['event_time'] >= datetime.datetime(year=2022, month=7, day=1)) & (data['event_time'] <= datetime.datetime(year=2023, month=5, day=1))]

    return paid_data

paid_data = read_paid()


from_date = datetime.date(2023,1,1)
to_date = datetime.date(2023, 3, 31)

total_data = pd.concat([organic_data, paid_data])
total_data['event_date'] = pd.to_datetime(total_data['event_time']).dt.date
total_data = total_data.loc[(total_data['event_date']>=from_date)&(total_data['event_date']<=to_date)]

class segment_analysis():
    def __init__(self, raw_data):
        self.raw_data = raw_data
        self.event_dict = {
            'loan_contract_completed' : {
                'seg_name' : 'contract',
                'period' : 30
            },
            'Viewed LA Home No Result' : {
                'seg_name' : 'VLHNo',
                'period' : 60
            },
        }
        self.result_data = None

    def update_conversion_data(self):
        df = self.raw_data.copy()

        base_data = df.loc[df['event_name'].isin(['re-engagement', 're-attribution'])]
        base_data = base_data.rename(columns={'event_date': 'conversion_date'})
        base_data = base_data.drop_duplicates(['event_time', 'appsflyer_id', 'event_name'])
        base_data['Cnt'] = 1
        base_data_pivot = base_data.pivot_table(index=['media_source', 'conversion_date', 'appsflyer_id'],
                                                values='Cnt',
                                                aggfunc='sum').reset_index()

        self.conversion_data = base_data_pivot
        self.result_data = self.conversion_data

    def make_segment_dataset(self, target_event, seg_name):
        df = self.raw_data.copy()

        seg_df = df.loc[df['event_name'] == target_event]
        seg_df['segment'] = seg_name
        seg_df = seg_df.drop_duplicates(['appsflyer_id', 'event_date'])
        seg_df = seg_df[['event_date', 'segment', 'appsflyer_id']]

        return seg_df

    def segment_match(self):
        conversion_data = self.conversion_data.copy()

        for target_event in self.event_dict.keys() :
            seg_name = self.event_dict['seg_name']
            target_period = self.event_dict['period']

            segment_df = self.make_segment_dataset(target_event, seg_name)
            col_name = f'{seg_name}_in_{str(target_period)}_days'

            merge_data = conversion_data.merge(segment_df, on='appsflyer_id', how='left')
            merge_data['time_gap'] = merge_data['conversion_date'] - merge_data['event_date']
            merge_data['time_gap'] = merge_data['time_gap'].apply(lambda x: x.days)

            merge_data.loc[(merge_data['time_gap'] <= target_period) & (merge_data['time_gap'] > 0), col_name] = True
            merge_data = merge_data.loc[merge_data[col_name] == True]

            merge_data_dedup = merge_data[['appsflyer_id', 'conversion_date', col_name]]
            merge_data_dedup = merge_data_dedup.drop_duplicates(['appsflyer_id', 'conversion_date', col_name])

            result_data = self.result_data.merge(merge_data_dedup, on=['appsflyer_id', 'conversion_date'],
                                                          how='left')
            result_data[col_name] = result_data[col_name].fillna(False)

            self.result_data = result_data










