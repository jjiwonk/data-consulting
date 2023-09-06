from setting import directory as dr
from workers.func import FunnelDataGenerator
from workers.func import SankeyModeling
from analysis.DCT1634_musicow_general.raw_data import data_set

data = data_set.data
install_user_data = data_set.install_user_data


install_user_pivot = install_user_data.pivot_table(index='Event Name', columns='dedup_order', values='Cnt',
                                                   aggfunc='sum').reset_index()

sign_up_event = install_user_data.loc[install_user_data['Event Name']=='af_signup_success']

print(len(install_user_data['unique_user_id'].unique()))
print(len(sign_up_event['unique_user_id'].unique()))
print(len(install_user_data['unique_user_id'].unique()) - len(sign_up_event['unique_user_id'].unique()))


sign_up_event_pivot = sign_up_event.pivot_table(index = 'Install Source',
                                               columns = 'ITET Group',
                                               values = 'Cnt',
                                               aggfunc = 'sum',
                                                 margins = True)
sign_up_event_pivot = sign_up_event_pivot[['0d', '7d', '30d', '30d+', 'All']]
sign_up_event_pivot = sign_up_event_pivot.reset_index()
sign_up_event_pivot = sign_up_event_pivot.sort_values('All', ascending=False)

## 이탈 퍼널 찾아보기
install_user_data['Event Name'].value_counts()

funnel_gen = FunnelDataGenerator(user_array = list(install_user_data['unique_user_id']),
                                  event_array = list(install_user_data['Event Name']),
                                  event_time_array= list(install_user_data['Event Time']),
                                  value_array= list(install_user_data['Cnt']),
                                  media_array= list(install_user_data['Media Source']),
                                  kpi_event_name='af_market_buy',
                                  funnel_period=30*24*3600,
                                  paid_events=['install'],
                                  add_end_sequence=True)

funnel_gen.do_work()
funnel_data = funnel_gen.data
funnel_data['session_sequence_string'] = funnel_data['funnel_sequence'].apply(lambda x : ' > '.join(x))

sankey = SankeyModeling(funnel_data,
                        funnel_list=['install', 'af_signup_intro', 'af_signup_success', 'af_market_buy'],
                        end_sequence='funnel_end',
                        sequence_column_name='session_sequence_string',
                        destination=dr.download_dir,
                        file_name='musicow_sankey.xlsx')
sankey.do_work()

sankey_data = sankey.data
sankey_data['filter'] = sankey_data['funnel_sequence'].apply(lambda x : True if x[0] == 'install' else False)
sankey_data['Install Source'] = sankey_data['media_sequence'].apply(lambda x : x[0])
sankey.data = sankey_data.loc[sankey_data['filter']==True]

sankey.sankey_to_excel()