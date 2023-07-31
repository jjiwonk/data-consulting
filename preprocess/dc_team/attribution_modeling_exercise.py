from setting import directory as dr
import pandas as pd
import numpy as np
from workers import func
from workers import attribution_model
import datetime

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/코오롱/RAW'
install_df = pd.read_csv(raw_dir + '/install.csv')
df = pd.read_csv(raw_dir + '/ua.csv', nrows=500000)
df = pd.concat([install_df, df], sort=False)

obs_event = 'af_purchase'
df['obs'] = df.apply(lambda x: 1 if x["event_name"] == obs_event else 0, axis=1)
df = df.loc[(df['obs'] == 1) |
            ((df['obs'] == 0) & (df['event_name'].isin(['install'])))]
df['media_source'] = df['media_source'].fillna('None')
df['event_time'] = pd.to_datetime(df['event_time'])
df['Cnt'] = 1
df = df.sort_values(['appsflyer_id', 'event_time'])

funnel_gen = func.FunnelDataGenerator(user_array=list(df['appsflyer_id']),
                                      event_array = list(df['event_name']),
                                      event_time_array=list(df['event_time']),
                                      value_array=list(df['Cnt']),
                                      media_array=list(df['media_source']),
                                      kpi_event_name='af_purchase',
                                      funnel_period=7*24*3600,
                                      paid_events=['install'],
                                      contribute='last',
                                      add_end_sequence=False)

funnel_gen.do_work()
funnel_data = funnel_gen.data
funnel_data = funnel_data.rename(columns = {'media_sequence' : 'path'})
funnel_data['conv'] = funnel_data['kpi_achievement'].astype('int')

basic_attr= attribution_model.BasicAttribution(funnel_data, 'conv')

linear = basic_attr.linear_attribution()
linear.rename(columns = {'conv' : 'linear'}, inplace=True)
last_click = basic_attr.last_click_attribution()
last_click.rename(columns = {'conv' : 'last_click'}, inplace=True)

markov = attribution_model.MarkovChainAttribution(funnel_data, np.sum(funnel_data['conv']), null_exists=True)
markov_result = markov.run(media_list = list(df['media_source'].unique()))
markov_result = pd.DataFrame(pd.Series(markov_result))
markov_result.columns = ['markov']

compare_df = pd.concat([linear, last_click, markov_result], axis = 1)


# total_conv = np.sum(df.loc[df['event_name'] == obs_event, 'Cnt'])
#
# media_event_pivot = df.pivot_table(index='media_source', columns='event_name', values='Cnt', aggfunc='sum')
# media_event = media_event_pivot.fillna(0).to_dict('index')
#
# info_data = df[['appsflyer_id', 'event_time', 'media_source', 'event_name']]
# info_data['conv'] = info_data['event_name'].apply(lambda x: 1 if x == obs_event else 0)
# info_data = info_data.sort_values(['appsflyer_id', 'event_time'])
# info_data['info'] = info_data.apply(lambda x: [x.event_time, x.media_source, x.conv], axis=1)
#
# first_conv_time = info_data.loc[info_data['conv'] == 1]
# first_conv_time = first_conv_time.drop_duplicates(['appsflyer_id', 'conv'], keep='first')
# first_conv_time.rename(columns={'event_time': 'first_conv_time'}, inplace=True)
#
# info_data_merge = info_data.merge(first_conv_time[['appsflyer_id', 'first_conv_time']], on='appsflyer_id',
#                                   how='left')
# info_data_merge = info_data_merge.loc[info_data_merge['first_conv_time'].notnull()]
# info_data_merge = info_data_merge.loc[info_data_merge['event_time'] <= info_data_merge['first_conv_time']]
#
# af_dict = dict()
#
# for row in info_data_merge.itertuples():
#     if not row.appsflyer_id in af_dict.keys():
#         af_dict[row.appsflyer_id] = list()
#     af_dict[row.appsflyer_id].append(row.info)
#
# result_df = {
#     id: {"path": [ent[1] for ent in val], "conv": sum([ent[2] for ent in val])} for id, val in af_dict.items()
# }
# result_df = pd.DataFrame(result_df).transpose()