import pandas as pd
import numpy as np

def user_identifier(df, platform_id, user_id):
    df = df.loc[df[user_id].str.len()>0]
    df = df.drop_duplicates([platform_id, user_id])

    pid_array = np.array(df[platform_id])
    pid_dict = {}

    uid_array = np.array(df[user_id])
    uid_dict = {}

    num = 1
    for idx, pid in enumerate(pid_array) :
        uid = uid_array[idx]

        if pid not in pid_dict.keys() :
            if uid in uid_dict.keys() :
                user_name = uid_dict.get(uid)
                pid_dict[pid] = user_name
            else :
                user_name = 'user ' + str(num)
                pid_dict[pid] = user_name
                uid_dict[pid] = user_name
                num += 1

    return pid_dict