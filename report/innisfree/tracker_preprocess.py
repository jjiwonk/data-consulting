from report.innisfree import directory as dr
from report.innisfree import ref

import pandas as pd
import pyarrow.csv as pacsv
import pyarrow as pa
import numpy as np
import datetime


def get_apps_log_data():
    apps_dir = dr.report_dir + '/appsflyer_prism'

    from_date = ref.report_date.start_date
    to_date = ref.report_date.target_date

    date = from_date
    dtypes = ref.columns.apps_dtype
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding = 'utf-8-sig')
    table_list = []

    while date <= to_date :
        date_name = date.strftime('%Y%m%d')
        file_name = f'appsflyer_daily_report_{date_name}.csv'

        table = pacsv.read_csv(apps_dir + '/' + file_name, convert_options=convert_ops, read_options=ro)
        table_list.append(table)

        print(file_name + ' Read 완료')

        date = date + datetime.timedelta(1)

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas()
    return raw_data

def apps_log_data_prep():
    raw_data = get_apps_log_data()

    target_event_list = list(ref.apps_info.target_event_dict.keys())
    raw_data = raw_data.loc[raw_data['event_name'].isin(target_event_list)]
    raw_data = raw_data.loc[raw_data['is_primary_attribution']!='False']
    raw_data = raw_data.loc[raw_data['partner'].isin(['madit',''])]
    raw_data = raw_data.loc[raw_data['attributed_touch_type']!='impression']

    raw_data[['attributed_touch_time', 'install_time', 'event_time']] = raw_data[['attributed_touch_time', 'install_time', 'event_time']].apply(lambda x: pd.to_datetime(x))
    raw_data = raw_data.sort_values('event_time')
    raw_data = raw_data.drop_duplicates(['appsflyer_id', 'event_name', 'event_time'])

    raw_data['date'] = raw_data['event_time'].dt.date
    raw_data['platform'] = raw_data['platform'].apply(lambda x : 'aos' if x == 'android' else 'ios')

    raw_data.loc[raw_data['attributed_touch_time'].isnull(), 'attributed_touch_time'] = raw_data['install_time']

    raw_data['CTIT'] = raw_data['install_time'] - raw_data['attributed_touch_time']
    raw_data['ITET'] = raw_data['event_time'] - raw_data['install_time']

    raw_data = raw_data.loc[(raw_data['CTIT'] < datetime.timedelta(ref.apps_info.ctit))&
                            (raw_data['ITET'] < datetime.timedelta(ref.apps_info.itet))]

    for col in ref.columns.apps_pivot_columns :
        raw_data[col] = raw_data[col].fillna('')

    raw_data['cnt'] = 1
    raw_data['event_revenue'] = pd.to_numeric(raw_data['event_revenue'])

    raw_data_pivot = raw_data.pivot_table(index = ref.columns.apps_pivot_columns, columns = 'event_name', values='cnt',aggfunc = 'sum')
    revenue_pivot = raw_data.loc[raw_data['event_name']=='af_purchase'].pivot_table(index = ref.columns.apps_pivot_columns, columns = 'event_name', values = 'event_revenue', aggfunc='sum')
    revenue_pivot = revenue_pivot.rename(columns = {'af_purchase' : 'Revenue_app'})

    total_pivot = pd.concat([raw_data_pivot, revenue_pivot], sort = False)
    total_pivot = total_pivot.fillna(0)
    total_pivot['Open'] = total_pivot['install'] + total_pivot['re-engagement'] + total_pivot['re-attribution']
    total_pivot = total_pivot.rename(columns = ref.apps_info.target_event_dict).reset_index()
    total_pivot = total_pivot[ref.columns.apps_result_columns]
    return total_pivot

def get_apps_agg_data():
    apps_dir = dr.report_dir + '/appsflyer_aggregated_prism'

    date = ref.report_date.start_date
    to_date = ref.report_date.target_date

    df_list = []
    while date <= to_date :
        date_name = date.strftime('%Y%m%d')
        d7_date = (date - datetime.timedelta(6)).strftime('%Y-%m-%d')
        file_name = f'appsflyer_aggregated_report_{date_name}.csv'
        df = pd.read_csv(apps_dir + '/' + file_name)
        print(file_name + ' Read 완료')

        if date != to_date :
            df = df.loc[df['date']==d7_date]
        else :
            pass

        df_list.append(df)
        date = date + datetime.timedelta(1)

    total_df = pd.concat(df_list)
    total_df = total_df.loc[pd.to_datetime(total_df['date']).dt.month == to_date.month]
    total_df = total_df.loc[total_df["media_source_pid"].isin(ref.apps_info.agg_data_media_filter)]
    total_df = total_df[ref.apps_info.agg_data_column_order]
    return total_df


def get_ga_data(report_type):
    ga_dir = dr.report_dir + '/GA'

    from_date = ref.report_date.start_date
    to_date = ref.report_date.target_date

    date = from_date
    df_list = []

    while date <= to_date:
        date_name = date.strftime('%Y%m%d')
        if report_type == 'trg':
            file_name = f'166897841_d7_report_{date_name}.csv'
        elif report_type == 'non_trg' :
            file_name = f'166897841_d7_non_trg_report_{date_name}.csv'

        data = pd.read_csv(ga_dir + '/' + file_name, encoding='utf-8-sig')
        data['date'] = date
        df_list.append(data)

        print(file_name + ' Read 완료')

        date = date + datetime.timedelta(1)

    raw_data = pd.concat(df_list, sort=False, ignore_index=True)

    for col in ref.columns.ga_dimension_cols :
        if col not in raw_data.columns :
            raw_data[col] = ''
        else :
            raw_data[col] = raw_data[col].fillna('')

    for col in ref.columns.ga_metric_cols :
        if col not in raw_data.columns :
            raw_data[col] = 0
        else :
            raw_data[col] = raw_data[col].fillna(0)

    raw_data = raw_data.loc[raw_data['dataSource'] == 'web']
    raw_data['check_sourcemedium'] = raw_data['sourceMedium'].apply(lambda x: ('organic' not in x) & ('referral' not in x))
    raw_data = raw_data.loc[raw_data['check_sourcemedium'] == True]
    raw_data = raw_data.drop(columns='check_sourcemedium')
    raw_data = raw_data[ref.columns.ga_dimension_cols + ref.columns.ga_metric_cols]
    return raw_data


def ga_prep():
    except_dimension50_cols = ref.columns.ga_dimension_cols
    except_dimension50_cols = list(set(except_dimension50_cols) - set(['dimension50']))

    trg_df = get_ga_data('trg')
    mapping_key = list(trg_df[except_dimension50_cols].values)

    # non_trg 데이터는 trg 데이터와의 매핑 가능 여부에 따라 둘로 나눔
    non_trg_df = get_ga_data('non_trg').set_index(except_dimension50_cols)

    # dimension50 매핑 불가 데이터
    non_mapping_data = non_trg_df.loc[~non_trg_df.index.isin(mapping_key)].reset_index()

    # dimension50 매핑 데이터
    mapping_data = non_trg_df.loc[non_trg_df.index.isin(mapping_key)].reset_index()

    trg_df_reverse = trg_df.copy()
    trg_df_reverse[ref.columns.ga_metric_cols] = trg_df[ref.columns.ga_metric_cols].apply(lambda x : x * -1)
    trg_df_reverse['dimension50'] = ''

    mapping_data = pd.concat([mapping_data, trg_df_reverse])
    mapping_data = mapping_data.pivot_table(index =except_dimension50_cols, values = ref.columns.ga_metric_cols, aggfunc = 'sum').reset_index()

    total_df = pd.concat([non_mapping_data,mapping_data, trg_df])
    total_df['deviceCategory'] = total_df['deviceCategory'].apply(lambda x: ref.ga_info.ga_device_dict.get(x))

    for col in ref.columns.ga_dimension_cols :
        if col in total_df.columns :
            total_df[col] = total_df[col].fillna('')
        else :
            total_df[col] = ''

    total_df_pivot = total_df.pivot_table(index = ref.columns.ga_dimension_cols, values = ref.columns.ga_metric_cols, aggfunc = 'sum').reset_index()
    total_df_pivot = total_df_pivot.loc[total_df_pivot[ref.columns.ga_metric_cols].values.sum(axis=1) > 0, :]
    total_df_pivot = total_df_pivot.rename(columns=ref.ga_info.ga_rename_dict)[ref.ga_info.ga_rename_dict.values()]
    return total_df_pivot