import pandas as pd
from datetime import datetime
import re
import os
import numpy as np
from setting import directory as dr

log_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/pjt_dc/logs'
file_list = os.listdir(log_dir)

raw_data = []
for file in file_list :
    f = open(log_dir + '/' + file,'r',encoding='utf-8-sig')
    content = f.read()

    time_pat = re.compile('[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2},[0-9]{3}')
    split_content_by_time = time_pat.split(content)
    time = time_pat.findall(content)

    time_series = pd.Series(time, name = 'timestamp')
    log_series = pd.Series(split_content_by_time[1:], name = 'log')

    temp = pd.concat([time_series, log_series], axis = 1)
    raw_data.append(temp)

data = pd.concat(raw_data)

data['timestamp'] = data['timestamp'].apply(lambda x : datetime.strptime(x.split(',')[0], '%Y-%m-%d %H:%M:%S'))
data['level_name'] = data['log'].apply(lambda x: x.split('\t')[0].strip())

stat_pat = re.compile('\(.+:.+:.+\)')
data['status'] = data['log'].apply(lambda x: stat_pat.findall(x)[0])
data['file_name'] = data['status'].apply(lambda x : x.split(':')[0][1:-3])
data['func_name'] = data['status'].apply(lambda x : x.split(':')[1])
data['lineno'] = data['status'].apply(lambda x : x.split(':')[2][:-1])

data['messages'] = data['log'].apply(lambda x: ''.join(x.split(stat_pat.findall(x)[0])[1:]))
data['job_name'] = data['messages'].apply(lambda x: x.split(':')[0].strip()[:-3])
data['message'] = data['messages'].apply(lambda x: ''.join(x.split(':')[1:]).lstrip())

data = data.drop(['log', 'status', 'messages'], axis = 1)
data = data.sort_values(['job_name', 'timestamp'])
data.index = range(len(data))


compare = data[['timestamp', 'job_name']]
compare = compare.iloc[1:]
compare = compare.append(compare.iloc[0])
compare.index = range(len(compare))
compare.columns = [col + '_comp' for col in compare.columns]


data_concat = pd.concat([data, compare], axis = 1,sort=False)
data_concat['time_gap'] = data_concat['timestamp_comp'] - data_concat['timestamp']
data_concat['time_gap'] = data_concat['time_gap'].apply(lambda x : x.total_seconds())

origin_array = np.array(data_concat['job_name'])
comp_array = np.array(data_concat['job_name_comp'])
timegap_array = np.array(data_concat['time_gap'])

session_list = []
session = 0
is_start_list = []
is_start = True
is_end_list = []
is_end = False
for i in range(len(data_concat)) :
    if i == 0:
        pass
    else :
        if origin_array[i] == comp_array[i] :
            if timegap_array[i-1] > 1800 :
                session = 0
                is_start = True
                is_end_list[i - 1] = True
            else :
                is_start = False
                session += 1
        else :
            session = 0
            is_start = True
            is_end_list[i-1] = True
    session_list.append(session)
    is_start_list.append(is_start)
    is_end_list.append(is_end)

data_concat['session'] = session_list
data_concat['is_start'] = is_start_list
data_concat['is_end'] = is_end_list
data_concat['message'] = data_concat['message'].apply(lambda x : f"{x}")
data_concat.to_excel(dr.download_dir + "/log_data.xlsx",index=False, encoding = 'utf-8-sig')