import pandas as pd
import os
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import setting.directory as dr
import setting.report_date as rdate

raw_dir = dr.dropbox_dir + f'/광고사업부/4. 광고주/알바몬/4-1. 광고주 제공자료/애드저스트 RAW/{rdate.month_name}'
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/잡코리아/result'

def deduction_data():
    folder = ['디덕션용']

    dtypes = {
        'activity_kind': pa.string(),
        'event_name': pa.string(),
        'app_version': pa.string(),
        'network_name': pa.string(),
        'created_at': pa.string(),
        'engagement_time':pa.string(),
        'adid': pa.string(),
        'Country': pa.string(),
        'language': pa.string(),
        'mcc' : pa.string(),
        'mnc': pa.string(),
        'device_manufacturer' : pa.string(),
        'app_version' : pa.string(),
        'sdk_version': pa.string(),
        'os_version': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20)
    table_list = []

    for period in folder:
        daily_dir = raw_dir + '/' + period
        daily_files = os.listdir(daily_dir)
        daily_files = [f for f in daily_files if '.csv' in f]

        for f in daily_files:
            temp = pacsv.read_csv(daily_dir + '/' + f, convert_options=convert_ops, read_options=ro)
            table_list.append(temp)
        print(period + ' Read 완료')

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas()
    raw_data = raw_data.loc[raw_data['network_name'].isin(['Cauly', 'Appier', 'RTB house'])]

    return raw_data

def deduction_calculate(df,file_name):
    target_event = ['간편문자지원', '개인 회원가입', '무료공고등록', '유료공고등록', '이력서작성_최초등록', '이력서작성_추가등록', '온라인 지원',
             '이메일지원', '전화지원', '기업 회원가입', '홈페이지지원', '일반문자지원']
    event_df = df.loc[df['event_name'].isin(target_event)]

    event_df.index = range(0, len(event_df))
    event_df['ITET'] = (pd.to_datetime(event_df['created_at']) - pd.to_datetime(event_df['engagement_time'])).apply(lambda x : x.days)

    event_df['app_name'] = event_df['sdk_version'].apply(lambda x : 'AOS' if 'android' in x else 'iOS')
    event_df['mcc'] = event_df['mcc'].astype('str')
    event_df['mnc'] = event_df['mnc'].apply(lambda x : '0' + str(x) if len(x)==1 else x)
    event_df['mccmnc'] = event_df['mcc'] + event_df['mnc']

    data_arr = np.array(event_df['mccmnc'])
    for i, data in enumerate(data_arr):
        try :
            ret = int(data)
        except :
            ret = data
        data_arr[i] = ret
    event_df['mccmnc'] = data_arr

    event_df['os'] = event_df['os_version'].apply(lambda x: int(x.split('.')[0]) if x!='' else 0)

    event_df['con0_D0'] = 0
    event_df.loc[(event_df['ITET']>0) | (pd.to_datetime(event_df['engagement_time']).dt.month!=rdate.day_1.month)|
                 (pd.to_datetime(event_df['created_at']).dt.date>rdate.start_day), 'con0_D0'] = 1

    event_df.loc[event_df['Country']!='kr', 'con1_country'] = 1
    event_df.loc[event_df['language']!='ko', 'con2_language'] = 1
    event_df.loc[~(event_df['mccmnc'].isin([45002,45007,45006,45004,45007,45003,45005,45012,45011,45008,45010,4502,4507,4506,4504,4503,4505,4508])), 'con3_mccmnc'] = 1
    event_df.loc[(event_df['network_name']=='Cauly')&(event_df['mccmnc']==45010), 'con3_mccmnc'] = 0
    device_list = ['allwinner','GIONEE','HONOR','HUAWEI','Infinix','iQOO','Itel',
                   'OnePlus','OPPO','POCO','realme','Redmi','sharp','TECNO','TGnCo','tufen','umidigi','vivo','Xiaomi','ZTE','Xiaomi','Huawei']
    event_df.loc[(event_df['device_manufacturer'].str.len()>0)&(event_df['device_manufacturer'].isin(device_list)), 'con4_device'] = 1
    event_df.loc[((event_df['app_name']=='AOS')&(event_df['os']<9)) | ((event_df['app_name']=='iOS')&(event_df['os']<14)), 'con5_os'] = 1

    def app_version_check(row):
        os = row['app_name']
        version = row['app_version'].split('.')

        if len(version) < 3:
            return 1

        p1 = int(version[0])
        p2 = int(version[1])
        p3 = int(version[2])

        if os == 'AOS':
            if (p1<4) | ((p1==4)&(p2<3)) | ((p1==4)&(p2==3)&(p3<5)):
                return 1
            else :
                return 0
        else :
            if (p1<6) | ((p1==6)&(p2<3)) | ((p1==6)&(p2==3)&(p3<4)):
                return 1
            else :
                return 0

    event_df['con6_appv'] = event_df.apply(app_version_check, axis = 1)
    event_df['con7_sdkv'] = 0

    sort_df = event_df[['adid', 'event_name', 'created_at']].sort_values(['adid', 'event_name', 'created_at'])
    sort_df['created_at'] = pd.to_datetime(sort_df['created_at'])
    sort_df.index = range(0, len(sort_df))

    reattr_df = df.loc[df['activity_kind'] == 'reattribution',['adid', 'created_at']]
    reattr_df = reattr_df.sort_values(['adid', 'created_at'])

    reattr_df['created_at'] = pd.to_datetime(reattr_df['created_at'])
    reattr_df.index = range(0, len(reattr_df))

    id_arr = np.array(reattr_df['adid'])
    time_arr = np.array(reattr_df['created_at'])

    num_list = []
    num_of_event = 0
    for i, id in enumerate(id_arr) :
        if i != 0 :
            if id != id_arr[i-1]: # 이전 ID와 달라진 경우
                first_time = time_arr[i] # ID가 바뀌었으니 first time 초기화
                num_of_event = 0 # 이벤트 횟수도 초기화

            else : # 이전 ID와 동일한 경우
                if (time_arr[i] - time_arr[i-1]).astype('timedelta64[m]') >= np.timedelta64(1, 'm'): #이전 이벤트와 1시간 이상 차이가 나는 경우
                    first_time = time_arr[i] # 1시간 지났으니 first time 초기화
                    num_of_event = 0 # 이벤트도 초기화
                else : # 이전 이벤트와 1시간 차이는 안남
                    time_gap = (time_arr[i] - first_time) # 첫번째 이벤트 시간과 비교
                    if time_gap.astype('timedelta64[m]') < np.timedelta64(1, 'm') : # 1시간 이내라면
                        num_of_event += 1
                    else :
                        first_time = time_arr[i-1]
                        time_gap = (time_arr[i] - first_time)
                        num_of_event = 0
                        if time_gap.astype('timedelta64[m]') < np.timedelta64(1, 'm'):  # 1시간 이내라면
                            num_of_event += 1

        else :
            first_time = time_arr[0] # 가장 첫번째 first_time
        num_list.append(num_of_event)

    temp = pd.DataFrame(num_list, columns = ['num_of_event'])
    reattr_df_concat = pd.concat([reattr_df, temp], axis = 1)
    over3_reattr_user = list(reattr_df_concat.loc[reattr_df_concat['num_of_event']>=2, 'adid'].unique())
    event_df.loc[event_df['adid'].isin(over3_reattr_user), 'con8_over3(reattr)'] = 1

    event_df = event_df.fillna(0)
    event_df.index = range(0, len(event_df))
    fraud_columns = ['con0_D0', 'con1_country', 'con2_language', 'con3_mccmnc', 'con4_device',
       'con5_os', 'con6_appv', 'con7_sdkv', 'con8_over3(reattr)']
    event_df.loc[event_df[fraud_columns].values.sum(axis = 1)>=1, 'is_fraud'] = 1
    event_df['is_fraud'] = event_df['is_fraud'].fillna(0)

    event_df.to_csv(result_dir + '/' + file_name, index=False, encoding = 'utf-8-sig')

df = deduction_data()
file_name = f'{rdate.month_name} 디덕션 적용.csv'
deduction_calculate(df, file_name)
