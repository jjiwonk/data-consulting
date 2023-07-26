import numpy as np
import pandas as pd
from setting import directory as dr
import datetime

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/Latib'

total_log = pd.read_csv(raw_dir + '/total_log.csv')
total_log = total_log.sort_values('주문일시')
total_log['date'] = pd.to_datetime(total_log['주문일시']).dt.date
total_log['address'] = total_log['수령인 주소'] + ' ' + total_log['수령인 상세주소']
total_log = total_log.loc[total_log['주문자명'].notnull()]
total_log['상품명(한국어 쇼핑몰)'] = total_log['상품명(한국어 쇼핑몰)'].fillna('')
total_log['총 결제금액(KRW)'] = total_log['총 결제금액(KRW)'].astype('int')

total_log.index = range(len(total_log))

# 주문 번호 생성
user_name_arr = np.array(total_log['주문자명'])
event_time_arr = np.array(total_log['주문일시'])
event_date_arr = np.array(total_log['date'])
result_array = np.empty(shape = len(total_log), dtype='object')

order_cnt = 0

for i, event_time in enumerate(event_time_arr) :
    user_name = user_name_arr[i]
    date = event_date_arr[i]

    if i == 0 :
        before_user = user_name
        before_event_time = event_time
        before_date = date

    else :
        if before_date != date :
            order_cnt = 0

        else :
            if (before_user == user_name) & (before_event_time == event_time) :
                pass
            else :
                order_cnt += 1

        before_user = user_name
        before_event_time = event_time
        before_date = date

    order_number = '{}-{:04d}'.format(date,order_cnt)
    result_array[i] = order_number

total_log['order_id'] = result_array
total_log = total_log[list([total_log.columns[-1]]) + list(total_log.columns[0:-1])]

user_data = total_log[['주문자명', '주문자 핸드폰번호', 'address']].drop_duplicates()
user_data['주문자명'] = user_data['주문자명'].str.strip().str.lower().str.replace(' ', '')
user_data = user_data.sort_values('주문자명')

class UserIdGenerator() :
    def __init__(self):
        self.user_id = 0
        self.user_id_dict = {}
        self.user_name_to_phone_number = {}
        self.user_name_to_address = {}

    def update_user_id_dict(self, user_name, phone_number, address):
        self.user_id += 1

        if user_name not in self.user_id_dict :
            self.user_id_dict[user_name] = {}

        if address not in self.user_id_dict[user_name] :
            self.user_id_dict[user_name][address] = {}
        self.user_id_dict[user_name][address]['user_id'] = self.user_id

        if phone_number not in self.user_id_dict[user_name] :
            self.user_id_dict[user_name][phone_number] = {}
        self.user_id_dict[user_name][phone_number]['user_id'] = self.user_id

        return self.user_id
    def generate_user_id(self, user_name, phone_number, address):
        if user_name not in self.user_id_dict:
            return self.update_user_id_dict(user_name, phone_number, address)

        if phone_number in self.user_id_dict[user_name] :
            return self.user_id_dict[user_name][phone_number]['user_id']

        if address in self.user_id_dict[user_name]:
            return self.user_id_dict[user_name][address]['user_id']


        return self.update_user_id_dict(user_name, phone_number, address)

uid_gen = UserIdGenerator()

user_data['user_id'] = user_data.apply(lambda row: uid_gen.generate_user_id(row['주문자명'], row['주문자 핸드폰번호'], row['address']), axis=1)

total_log = total_log.merge(user_data, on = ['주문자명', '주문자 핸드폰번호', 'address'], how = 'left')

# 각 유저별 첫 구매 정보
unique_purchase_data = total_log.sort_values(['order_id', '주문일시'])
unique_purchase_data = total_log.drop_duplicates('order_id')
unique_purchase_data['Cnt'] = 1


first_purchase_data = unique_purchase_data.sort_values(['user_id', '주문일시'])
first_purchase_data = first_purchase_data.drop_duplicates(subset = ['user_id'],keep='first')
first_purchase_data = first_purchase_data[['order_id', 'user_id','date','주문일시', '총 결제금액(KRW)', '매출경로', '결제수단']]
first_purchase_data = first_purchase_data.rename(columns = {'주문일시' : '첫주문일시'})
first_purchase_pivot = first_purchase_data.pivot_table(index =['매출경로'], values = ['총 결제금액(KRW)', 'Cnt'], aggfunc='sum').reset_index()
first_purchase_order_id = first_purchase_data['order_id']

unique_purchase_data.loc[unique_purchase_data['order_id'].isin(first_purchase_order_id), '첫구매 여부'] = True
unique_purchase_data['첫구매 여부'] = unique_purchase_data['첫구매 여부'].fillna(False)
unique_purchase_pivot = unique_purchase_data.pivot_table(index =['첫구매 여부'], values = ['총 결제금액(KRW)', 'Cnt'], aggfunc='sum').reset_index()


non_first_purchase_data = unique_purchase_data.loc[unique_purchase_data['첫구매 여부']==False]
non_first_purchase_pivot = non_first_purchase_data.pivot_table(index =['매출경로'], values = ['총 결제금액(KRW)', 'Cnt'], aggfunc='sum').reset_index()


influencer_list = ['헬퀸', '연유샘', '나연']


df_list = []
for infl in influencer_list :
    temp = unique_purchase_data.loc[unique_purchase_data['상품명(한국어 쇼핑몰)'].str.contains(infl)]
    temp['인플루언서'] = infl
    df_list.append(temp)

infl_purchase_log = pd.concat(df_list, ignore_index= True)
infl_purchase_log = infl_purchase_log.sort_values(['인플루언서','user_id', 'date'])
infl_purchase_pivot = infl_purchase_log.pivot_table(index = ['인플루언서', '첫구매 여부'], values = ['Cnt', '총 결제금액(KRW)'], aggfunc = 'sum').reset_index()

infl_purchase_log_first = infl_purchase_log.loc[infl_purchase_log['첫구매 여부']==True]
infl_purchase_log_first = infl_purchase_log_first.sort_values(['주문일시'])
infl_purchase_log_first = infl_purchase_log_first.drop_duplicates(subset = 'user_id', keep='first')
infl_purchase_log_first = infl_purchase_log_first[['user_id', '인플루언서','주문일시']]
infl_purchase_log_first = infl_purchase_log_first.rename(columns = {'주문일시' : '첫 구매 일자'})

infl_user_purchase_data = unique_purchase_data.loc[unique_purchase_data['user_id'].isin(infl_purchase_log_first['user_id'])]
infl_user_purchase_data = infl_user_purchase_data.merge(infl_purchase_log_first, on = 'user_id', how = 'left')


infl_user_ltv_pivot = infl_user_purchase_data.pivot_table(index = ['인플루언서', '첫구매 여부'], values = ['Cnt', '총 결제금액(KRW)'], aggfunc = 'sum').reset_index()

daily_first_purchase_pivot = first_purchase_data.pivot_table(index = 'date',  values = ['Cnt', '총 결제금액(KRW)'], aggfunc = 'sum').reset_index()
daily_first_purchase_pivot.to_csv(raw_dir + '/daily_first_purchase_log.csv', index=False, encoding = 'utf-8-sig')


period1 = first_purchase_data.loc[(unique_purchase_data['date']>=datetime.date(2023,4,21))&
                                   (unique_purchase_data['date']<=datetime.date(2023,4,25))]
period1_user = period1['user_id']
period1_user_purchase = unique_purchase_data.loc[unique_purchase_data['user_id'].isin(period1_user)]
period1_user_purchase = period1_user_purchase.merge(infl_purchase_log_first, on ='user_id', how = 'left')
period1_user_purchase['인플루언서'] = period1_user_purchase['인플루언서'].fillna('간접')
period1_user_purchase = period1_user_purchase.loc[period1_user_purchase['date']<=datetime.date(2023,5,11)]
period1_user_purchase_pivot = period1_user_purchase.pivot_table(index = ['인플루언서', '첫구매 여부'], values = ['Cnt', '총 결제금액(KRW)'], aggfunc = 'sum').reset_index()

period2 = first_purchase_data.loc[(unique_purchase_data['date']>=datetime.date(2023,6,12))&
                                   (unique_purchase_data['date']<=datetime.date(2023,6,16))]
period2_user = period2['user_id']
period2_user_purchase = unique_purchase_data.loc[unique_purchase_data['user_id'].isin(period2_user)]
period2_user_purchase = period2_user_purchase.merge(infl_purchase_log_first, on ='user_id', how = 'left')
period2_user_purchase['인플루언서'] = period2_user_purchase['인플루언서'].fillna('간접')
period2_user_purchase_pivot = period2_user_purchase.pivot_table(index = ['인플루언서', '첫구매 여부'], values = ['Cnt', '총 결제금액(KRW)'], aggfunc = 'sum').reset_index()

## 구매 주기 계산

purchase_freq = non_first_purchase_data.sort_values(['user_id', '주문일시'])
purchase_freq = purchase_freq.merge(first_purchase_data[['user_id', '첫주문일시']], on='user_id', how='left')
purchase_freq = purchase_freq.drop_duplicates(subset = 'user_id', keep='first')
purchase_freq['time_gap'] = pd.to_datetime(purchase_freq['주문일시']) - pd.to_datetime(purchase_freq['첫주문일시'])
purchase_freq['time_gap'] = purchase_freq['time_gap'].apply(lambda x : x.days)

purchase_freq.loc[(purchase_freq['time_gap']<20) & ~(purchase_freq['user_id'].isin(period1_user)) & ~(purchase_freq['user_id'].isin(period2_user))]
first_purchase_data.loc[first_purchase_data['user_id'].isin(period1_user)]

purchase_freq['time_gap'].describe()
np.median(purchase_freq['time_gap'])
len(first_purchase_data['user_id'].unique())

item_data = total_log[['order_id', 'user_id', 'date','주문일시', '상품명(한국어 쇼핑몰)', '상품옵션', '매출경로', '결제수단']]
item_data = item_data.loc[item_data['user_id'].notnull()]
item_data.loc[item_data['order_id'].isin(first_purchase_order_id), '첫구매 여부'] = True
item_data = item_data.loc[item_data['상품명(한국어 쇼핑몰)']!='']
item_data['첫구매 여부'] = item_data['첫구매 여부'].fillna(False)
item_data['Cnt'] = 1
item_data.to_csv(raw_dir + '/item_data.csv', index=False, encoding = 'utf-8-sig')

item_data_pivot = item_data.pivot_table(index = '상품명(한국어 쇼핑몰)', columns = '첫구매 여부', values = 'Cnt', aggfunc = 'sum')

item_on_user = item_data.pivot_table(index = ['user_id', '상품명(한국어 쇼핑몰)'], values = 'Cnt', aggfunc = 'sum').reset_index()
item_on_user.to_csv(raw_dir + '/item_on_user.csv', index=False, encoding = 'utf-8-sig')

item_on_user_pivot = item_on_user.pivot_table(index = '상품명(한국어 쇼핑몰)', values = 'Cnt', aggfunc = 'mean')

item_pivot_agg = pd.concat([item_data_pivot, item_on_user_pivot], axis =1 )
item_pivot_agg= item_pivot_agg.reset_index()
item_pivot_agg = item_pivot_agg.fillna(0)
item_pivot_agg.to_csv(raw_dir + '/item_agg_pivot.csv', index=False, encoding = 'utf-8-sig')