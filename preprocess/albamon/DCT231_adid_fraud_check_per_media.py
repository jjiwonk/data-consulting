import pandas as pd
import os
import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv
import setting.directory as dr
import setting.report_date as rdate
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/알바몬/4-1. 광고주 제공자료/애드저스트 RAW/10월/1001-1031'
# raw_dir = 'D:/매드업/데이터분석/알바몬'
result_dir = dr.download_dir

def get_raw_data(raw_dir):
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
        'gps_adid' : pa.string(),
        'idfa': pa.string(),
        'app_name': pa.string(),
        'app_version_short': pa.string(),
        'sdk_version': pa.string(),
        'os_version': pa.string()
    }
    index_columns = list(dtypes.keys())
    convert_ops = pacsv.ConvertOptions(column_types=dtypes, include_columns=index_columns)
    ro = pacsv.ReadOptions(block_size=10 << 20, encoding='utf-8')
    table_list = []

    files = os.listdir(raw_dir)
    files = [f for f in files if '.csv' in f]

    for f in files:
        temp = pacsv.read_csv(raw_dir + '/' + f, convert_options=convert_ops, read_options=ro)
        table_list.append(temp)
    print('Raw data read successfully.')

    raw_data = pa.concat_tables(table_list)
    raw_data = raw_data.to_pandas()

    return raw_data


def fraud_calculate(raw_data):
    event_names = ["이력서작성_최초등록","이력서작성_추가등록","개인 회원가입","기업 회원가입","무료공고등록","유료공고등록","온라인 지원","간편문자지원","이메일지원","전화지원","홈페이지지원","일반문자지원"]
    check_df = raw_data.loc[raw_data['event_name'].isin(event_names)]
    check_df['adid'] = check_df.apply(lambda x: x['gps_adid'] if 'android' in x['sdk_version'] else x['adid'], axis=1)
    check_df[['created_at', 'engagement_time']] = check_df[['created_at', 'engagement_time']].apply(pd.to_datetime)
    check_df['date'] = check_df['created_at'].apply(lambda x: x.date())

    prep_df = check_df.pivot_table(index=['network_name', 'date', 'adid', 'event_name'], values='activity_kind',
                                   aggfunc='count').reset_index().rename(columns={'activity_kind': 'counts'})
    prep_df['over5_days'] = prep_df['counts'].apply(lambda x: 1 if x >= 5 else 0)
    prep_df['over10_days'] = prep_df['counts'].apply(lambda x: 1 if x >= 10 else 0)
    prep_df['over20_days'] = prep_df['counts'].apply(lambda x: 1 if x >= 20 else 0)

    prep_raw_df = prep_df.pivot_table(index=['network_name', 'adid'], columns='event_name',
                                      values=['counts', 'over5_days', 'over10_days', 'over20_days'],
                                      aggfunc='sum').reset_index().fillna(0)

    prep_raw_df[('is_fraud_over5', '')] = prep_raw_df['over5_days'].sum(axis=1).apply(lambda x: True if x > 0 else False)
    prep_raw_df[('is_fraud_over10', '')] = prep_raw_df['over10_days'].sum(axis=1).apply(lambda x: True if x > 0 else False)
    prep_raw_df[('is_fraud_over20', '')] = prep_raw_df['over20_days'].sum(axis=1).apply(lambda x: True if x > 0 else False)

    prep_raw_df = prep_raw_df[[('network_name',''), ('adid', ''),
                               ('counts', '간편문자지원'), ('counts', '개인 회원가입'), ('counts', '기업 회원가입'),
                               ('counts', '무료공고등록'), ('counts', '온라인 지원'), ('counts', '유료공고등록'),
                               ('counts', '이력서작성_최초등록'), ('counts', '이력서작성_추가등록'), ('counts', '이메일지원'),
                               ('counts', '전화지원'), ('counts', '홈페이지지원'),
                               ('is_fraud_over5', ''), ('is_fraud_over10', ''), ('is_fraud_over20', '')]]

    os_separator = raw_data.loc[:,['adid','app_name']].drop_duplicates()
    os_separator['os'] = os_separator['app_name'].apply(lambda x: 'AOS' if x == 'com.albamon.app' else 'IOS')
    prep_raw_df = prep_raw_df.merge(os_separator, how='left', on='adid')

    return prep_raw_df


def deduction_calculate(raw_data):
    target_event = ['간편문자지원', '개인 회원가입', '무료공고등록', '유료공고등록', '이력서작성_최초등록', '이력서작성_추가등록', '온라인 지원',
             '이메일지원', '전화지원', '기업 회원가입', '홈페이지지원', '일반문자지원']
    event_df = raw_data.loc[raw_data['event_name'].isin(target_event)]

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
                 (pd.to_datetime(event_df['created_at']).dt.date<rdate.start_day), 'con0_D0'] = 1

    event_df.loc[event_df['Country']!='kr', 'con1_country'] = 1
    event_df.loc[event_df['language']!='ko', 'con2_language'] = 1
    event_df.loc[~(event_df['mccmnc'].isin([45002,45007,45006,45004,45007,45003,45005,45012,45011,45008,45010,4502,4507,4506,4504,4503,4505,4508])), 'con3_mccmnc'] = 1
    event_df.loc[(event_df['network_name']=='Cauly')&(event_df['mccmnc']==45010), 'con3_mccmnc'] = 0

    # iOS 예외처리
    event_df.loc[event_df['app_name']=='iOS', ['con2_language', 'con3_mccmnc']] = 0

    device_list = ['allwinner','GIONEE','HONOR','HUAWEI','Infinix','iQOO','Itel',
                   'OnePlus','OPPO','POCO','realme','Redmi','sharp','TECNO','TGnCo','tufen','umidigi','vivo','Xiaomi','ZTE','Xiaomi','Huawei']
    event_df.loc[(event_df['device_manufacturer'].str.len()>0)&(event_df['device_manufacturer'].isin(device_list)), 'con4_device'] = 1
    event_df.loc[((event_df['app_name']=='AOS')&(event_df['os']<9)) | ((event_df['app_name']=='iOS')&(event_df['os']<14)), 'con5_os'] = 1

    def app_version_check(row):
        os = row['app_name']

        if os == 'AOS':
            col_name = 'app_version'
        elif os == 'iOS' :
            col_name = 'app_version_short'

        version = row[col_name].split('.')

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

    # adid AOS/IOS 구분 조건 추가
    event_df['adid'] = event_df.apply(lambda x: x['gps_adid'] if 'AOS' in x['app_name'] else x['adid'], axis=1)
    event_df = event_df.drop(['gps_adid','idfa'], axis=1)

    sort_df = event_df[['adid', 'event_name', 'created_at']].sort_values(['adid', 'event_name', 'created_at'])
    sort_df['created_at'] = pd.to_datetime(sort_df['created_at'])
    sort_df.index = range(0, len(sort_df))

    reattr_df = raw_data.loc[raw_data['activity_kind'] == 'reattribution',['adid', 'created_at']]
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
    fraud_columns = ['con1_country', 'con2_language', 'con3_mccmnc', 'con4_device',
       'con5_os', 'con6_appv', 'con7_sdkv', 'con8_over3(reattr)']
    event_df.loc[event_df[fraud_columns].values.sum(axis = 1)>=1, 'is_fraud'] = 1
    event_df['is_fraud'] = event_df['is_fraud'].fillna(0)
    checked_df = event_df.pivot_table(index=['network_name', 'adid'], columns='event_name', values=['con0_D0', 'is_fraud'], aggfunc='sum').reset_index().fillna(0)
    checked_df[('con0_D0', '')] = checked_df['con0_D0'].sum(axis=1).apply(lambda x: True if x > 0 else False)
    checked_df[('is_fraud_adid', '')] = checked_df['is_fraud'].sum(axis=1).apply(lambda x: True if x > 0 else False)
    checked_df = checked_df[[('network_name', ''), ('adid', ''), ('con0_D0', ''), ('is_fraud_adid', '')]]

    return checked_df
    # DCT111 코드 내 함수 활용


raw_data = get_raw_data(raw_dir)
raw_data = raw_data.loc[(raw_data['created_at'] >= '2022-10-01')&(raw_data['created_at'] < '2022-11-01')]

fraud_checked_df = fraud_calculate(raw_data)
deduction_checked_df = deduction_calculate(raw_data)
merged_df = pd.merge(fraud_checked_df, deduction_checked_df, how='outer', on=[('network_name', ''), ('adid', '')])

merged_df.loc[0:800000,:].to_excel(dr.download_dir + '/알바몬_미디어별_중복전환_ADID(1).xlsx', encoding='utf-8')
merged_df.loc[800000:,:].to_excel(dr.download_dir + '/알바몬_미디어별_중복전환_ADID(2).xlsx', encoding='utf-8')