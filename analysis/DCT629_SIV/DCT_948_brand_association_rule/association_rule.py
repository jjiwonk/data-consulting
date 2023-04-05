import os
import setting.directory as dr
import pyarrow as pa
import pandas as pd
from workers import read_data
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori, association_rules
import itertools

#data_prep에서 가공한 raw 불러오기
def raw_read():
    file_dir = dr.dropbox_dir+'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/raw/apps'
    file_list = os.listdir(file_dir)

    dtypes = {'주문번호': pa.string(),
              'appsflyer_id' : pa.string(),
                  '파트 구분': pa.string(),
                  '연도': pa.string(),
                  '월': pa.string(),
                  '주차': pa.string(),
                  '날짜': pa.date32(),
                  '매체': pa.string(),
                  '지면/상품': pa.string(),
                  '캠페인 구분': pa.string(),
                  'KPI': pa.string(),
                  '캠페인': pa.string(),
                  '세트': pa.string(),
                  '소재': pa.string(),
                  '머징코드': pa.string(),
                  '캠페인 라벨': pa.string(),
                  'OS': pa.string(),
                  '구매브랜드': pa.string(),
                  '구매상품': pa.string(),
                  '구매': pa.float64(),
                  '매출': pa.float64(),
                  '캠페인(인덱스)': pa.string(),
                  '세트(인덱스)': pa.string(),
                  '프로모션': pa.string(),
                  '브랜드': pa.string(),
                  '카테고리': pa.string(),
                  '소재형태': pa.string(),
                  '소재이미지': pa.string(),
                  '소재카피': pa.string()}

    df = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)

    return df

def item_extract():
    df = raw_read()

    df = df.sort_values(['appsflyer_id','날짜'])
    df.index = range(len(df))

    purchase_list = df.groupby('주문번호')['구매브랜드'].agg(lambda x: ','.join((x))).reset_index()
    purchase_list['구매브랜드'] =  purchase_list['구매브랜드'].apply(lambda x : x.split(','))

    purchase_dict = dict(zip(purchase_list['주문번호'],purchase_list['구매브랜드']))

    order_list = df.groupby('appsflyer_id')['주문번호'].agg(lambda x: ','.join(set(x))).reset_index()
    order_list['주문번호'] = order_list['주문번호'].apply(lambda x: x.split(','))

    for i in range(len(order_list)):
        ran = len(order_list['주문번호'][i])
        for n in range(ran):
            order_list['주문번호'][i][n] = purchase_dict[order_list['주문번호'][i][n]]

    order_list = order_list.rename(columns = {'주문번호':'구매브랜드리스트'})

    # 각 주문 번호 별로 이전 구매 날리고, 이 후 구매건만 집계하기!
    df['비교'] = df['appsflyer_id'].shift(1)
    df['num'] = 0

    for i in range(len(df)):
        compar = df['비교'][i]
        apps_id = df['appsflyer_id'][i]
        order_id = df['주문번호'][i]
        if i == 0 :
            num = 0
            df['num'][i] = num
        else:
            ex_num = df['num'][i - 1]
            ex_order_id = df['주문번호'][i -1]
            if compar == apps_id :
                if order_id == ex_order_id :
                    num = ex_num
                    df['num'][i] = num
                else:
                    num = ex_num + 1
                    df['num'][i] = num
            else:
                num = 0
                df['num'][i] = num

    merge_df = pd.merge(df, order_list , on = 'appsflyer_id', how = 'left')
    merge_df['order_brand'] = '-'

    for i in range(len(merge_df)):
        num = merge_df['num'][i]
        item_list = merge_df['구매브랜드리스트'][i]
        merge_df['order_brand'][i] = item_list[num:]

    merge_df['order_brand'] = merge_df['order_brand'].apply(lambda x : list(itertools.chain(*x)))
    merge_df['order_brand'] = merge_df['order_brand'].apply(lambda x: list(set(x)))

    return merge_df

#연관 규칙 분석
def association_rule(df,part):

    df = df.drop(columns=['구매브랜드', '구매상품', '비교', 'num', '구매브랜드리스트'])
    df = df.drop_duplicates(['주문번호', 'appsflyer_id'], keep='first')

    if part == 'all':
        pass
    else :
        df = df.loc[df['파트 구분'] == part]

    df = df.drop_duplicates(['appsflyer_id'], keep='first')

    item_list = df['order_brand'].to_list()

    #분석 시작
    te = TransactionEncoder()
    te_ary = te.fit(item_list).transform(item_list)
    df = pd.DataFrame(te_ary, columns=te.columns_)

    frequent_itemsets = apriori(df, min_support=0.01, use_colnames=True)
    frequent_itemsets = frequent_itemsets.sort_values('support', ascending=False)
    frequent_itemsets['파트'] = f'{part}'
    frequent_itemsets.to_csv(dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/result/{part}_frequent_itemsets.csv', encoding = 'utf-8-sig', index =False)

    ass_rule = association_rules(frequent_itemsets, metric="lift", min_threshold=0.01)
    ass_rule['파트'] = f'{part}'
    ass_rule.to_csv(dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/result/{part}_ass_rule.csv',encoding = 'utf-8-sig',index =False)

    return ass_rule

part_list = ['SIVILLAGE','JAJU','국내패션','글로벌패션','SIBEAUTY','all']

df = item_extract()

for i in part_list:
    result = association_rule(df, i )

# raw 취합

def frequent_read():
    file_dir = dr.dropbox_dir+'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/result/frequent_itemsets'
    file_list = os.listdir(file_dir)

    dtypes = {'support': pa.float64(),
              'itemsets' : pa.string(),
              '파트': pa.string()}

    df = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)

    return df

def ass_rule_read():
    file_dir = dr.dropbox_dir+'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/result/ass_rule'
    file_list = os.listdir(file_dir)

    dtypes = {'antecedents': pa.string(),
              'consequents' : pa.string(),
              'antecedent support': pa.float64(),
              'consequent support': pa.float64(),
              'support': pa.float64(),
              'confidence': pa.float64(),
              'lift': pa.float64(),
              'leverage': pa.float64(),
              'conviction': pa.float64(),
              '파트': pa.string()}

    df = read_data.pyarrow_csv(dtypes=dtypes, directory=file_dir, file_list=file_list)

    return df

