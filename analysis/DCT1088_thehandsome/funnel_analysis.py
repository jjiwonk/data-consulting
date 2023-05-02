import analysis.DCT1088_thehandsome.data_prep as prep
from workers import func

import pandas as pd
import numpy as np

# raw 데이터 로드 및 가공
total_df = prep.get_total_raw_data()
total_df.loc[total_df['event_value'] == ''] = '{}'
total_df['member_id'] = prep.get_event_from_values(np.array(total_df['event_value']), 'af_member_id')

user_id_dict = func.user_identifier(total_df, 'appsflyer_id', 'member_id')
total_df['uniquer_user_id'] = total_df['appsflyer_id'].apply(lambda x : user_id_dict.get(x))
temp = total_df.loc[~(total_df['uniquer_user_id'].str.len()>0)]
temp = temp.sort_values(['appsflyer_id', 'event_time'])