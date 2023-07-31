from setting import directory as dr
from workers import read_data
import pandas as pd
import pyarrow as pa
import os

raw_dir = dr.download_dir
ga_files = os.listdir(raw_dir + '/GA_데이터')

dtypes = {'Date' : pa.string(),
          'Part_Main' : pa.string(),
          'Part_Sub' : pa.string(),
          'fullVisitorId': pa.string(),
          'visitId': pa.string(),
          'userId': pa.string(),
          'visitNumber': pa.string(),
          'visitStartTime': pa.string(),
          'TrafficSource_source': pa.string(),
          'TrafficSource_medium': pa.string(),
          'pagePath': pa.string(),
          'action_type': pa.string(),
          'transactionId': pa.string(),
          'productRevenue': pa.string(),
          'productQuantity': pa.string(),
          'eventInfo_eventCategory': pa.string(),
          'eventInfo_eventAction': pa.string(),
          'eventInfo_eventLabel': pa.string(),
          'eventInfo_eventValue': pa.string()}

data = read_data.pyarrow_csv(directory=raw_dir + '/GA_데이터', dtypes=dtypes,file_list=ga_files)
data.loc[data['eventInfo_eventCategory']=='전자상거래 이벤트']['eventInfo_eventLabel'].value_counts()

transaction_data = data.loc[data['eventInfo_eventLabel']=='Transaction']
transaction_data = transaction_data.sort_values(['userId', 'Date'])
transaction_data = transaction_data.loc[transaction_data['userId']!='']
transaction_data['Cnt'] = 1
transaction_user_pivot = transaction_data.pivot_table(index = 'userId', values = 'Cnt', columns = 'TrafficSource_source', aggfunc = 'sum', margins=True).reset_index()
transaction_user_pivot = transaction_user_pivot.sort_values('facebook', ascending=False)
transaction_user_pivot = transaction_user_pivot.fillna(0)
transaction_user_pivot.to_csv(raw_dir + '/user_id_pivot.csv', index=False, encoding = 'utf-8-sig')

temp = data.loc[data['pagePath'].str.contains('utm_source=facebook')]

direct_user = data.loc[(data['TrafficSource_source']=='(direct)')&
                       (data['userId']!=''), 'userId'].unique()
direct_user_log = data.loc[data['userId'].isin(direct_user)]
