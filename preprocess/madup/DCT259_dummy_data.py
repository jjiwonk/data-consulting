import setting.directory as dr
import pandas as pd
import numpy as np

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/result/핀다'
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/Tableau/tableau/매드업'

file_list = ['tableau_creative_rd_finda_202207.csv', 'tableau_creative_rd_finda_202208.csv', 'tableau_creative_rd_finda_202209.csv']
df_list = []
for file_name in file_list :
    data = pd.read_csv(raw_dir + '/' + file_name)
    data['file_name'] = file_name
    df_list.append(data)

raw_data = pd.concat(df_list)

def name_convertor(df, except_list, column_name, dummy_name):
    df_copy = df.loc[~df[column_name].isin([except_list])]
    df_copy = df_copy.loc[df[column_name].notnull()]

    unique_list = list(df_copy[column_name].unique())
    dummy_list = [dummy_name + " " + str(i+1) for i in range(0, len(unique_list))]

    dummy_dict = dict(zip(unique_list, dummy_list))
    df[column_name] = df[column_name].apply(lambda x : dummy_dict.get(x) if x in unique_list else x)

    return df

raw_data.loc[raw_data['AD name']=='loantransfer-7', '소재 URL'] = 'https://iherb-creative-public.s3.ap-northeast-2.amazonaws.com/2022/%ED%85%8C%EC%8A%A4%ED%8A%B8/%EC%BA%A1%EC%B2%98.PNG'

except_list = ['ㅁ']
raw_data = name_convertor(raw_data, except_list,'캠페인', 'Campaign')
raw_data = name_convertor(raw_data, except_list,'광고그룹', 'Adset')
raw_data = name_convertor(raw_data, except_list,'소재', 'Ad')
raw_data = name_convertor(raw_data, except_list,'AD1', 'AD Category')
raw_data = name_convertor(raw_data, except_list,'AD2', 'AD Sub Category')
raw_data = name_convertor(raw_data, except_list,'AD name', 'AD name')



def generate_dummy_array(target_array, total_len):
    repeat = total_len // len(target_array)
    mod = total_len % len(target_array)
    dummy_array = target_array * (repeat)
    if mod == 0:
        pass
    else :
        last_array = target_array[:mod]
        dummy_array = dummy_array + last_array

    return dummy_array

target_array = [1, -1]
total_len = len(raw_data)

dummy_array = generate_dummy_array(target_array, total_len)

raw_data.index = range(0, total_len)

value_columns = ['노출', '클릭', '조회', '설치', '가입', '한도조회', '대출실행','마이데이터']
for col in value_columns :
    raw_data.loc[raw_data[col]>0, col] = raw_data[col] + dummy_array

result_file_list = ['tableau_creative_rd_202207.csv', 'tableau_creative_rd_202208.csv', 'tableau_creative_rd_202209.csv']
for i, file_name in enumerate(file_list) :
    save_data = raw_data.loc[raw_data['file_name']==file_name]
    save_data.to_csv(result_dir + '/' + result_file_list[i], index=False, encoding='utf-8-sig')




