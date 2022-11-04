import os
import pandas as pd


def create_folder(dir):
    try:
        if not os.path.exists(dir):
            os.makedirs(dir)
    except OSError:
        print('Error: Creating directory. ' + dir)


def union_raw_data(raw_dir, result_dir, file_name):
    total_df = pd.DataFrame()
    raw_files = os.listdir(raw_dir)
    for file in raw_files:
        temp = pd.read_excel(raw_dir + '/' + file)
        total_df = pd.concat([total_df, temp], axis=0)
    create_folder(result_dir)
    total_df.to_excel(result_dir + '/' + file_name, index=False, encoding='utf-8-sig')
    print('union successful')

