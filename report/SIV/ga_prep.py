from report.SIV import directory as dr
from report.SIV import ref
import datetime
import pandas as pd

def ga_read(type, use_col, ga4 = '',header= 0):
    dir = dr.report_dir + f'ga_{type}_prism{ga4}'

    from_date = ref.r_date.start_date
    to_date = ref.r_date.target_date

    ga_raw = pd.DataFrame()

    while from_date <= to_date : #리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        file_name = f'127881812_{type}_daily_report_{date_name}.csv'

        file = pd.read_csv(dir + '/' + file_name, usecols= use_col, header= header)
        file['날짜'] = from_date.strftime('%Y-%m-%d')
        ga_raw = ga_raw.append(file)

        print(file_name + ' Read 완료')

        from_date = from_date + datetime.timedelta(1)

    return ga_raw

def ga_exception(df):

    df['source'] = df['sourceMedium'].apply(lambda x: x.split(' / ')[0])
    df['medium'] = df['sourceMedium'].apply(lambda x: x.split(' / ')[-1]).drop(columns='sourceMedium')
    df = df.loc[df['source'].isin(ref.columns.ga1_media)]

    #예외처리(크리테오)
    df.loc[df['medium'] == 'da_app_If', 'medium'] = 'da_app_lf'

    criteo_dc = ['feed','(not set)','jaju_sales']
    criteo_s = ['criteo']

    df.index = range(len(df))
    idx = df[(df['source'].isin(criteo_s))&(df['campaign'].isin(criteo_dc))].index
    df.drop(idx, inplace = True)

    df.loc[(df['campaign'] == 'dynamic') & (df['medium'] == 'da_pc_lf'), 'campaign'] = 'LF - DESKTOP - CVO'

    #예외처리(시트 + 브검)
    c_media = ['google','naver','navershoppingsa']
    df.loc[df['source'].isin(c_media), 'campaign'] = df['campaign'].apply(lambda x: x.replace(x, ref.exc_cdict[x]) if x in ref.exc_cdict.keys() else x)

    a_media = ['criteo','rtbhouse_int']
    df.loc[df['source'].isin(a_media), 'adContent'] = df['adContent'].apply(lambda x: x.replace(x, ref.exc_ga_adict[x]) if x in ref.exc_ga_adict.keys() else x)

    # 자주 구글SA 예외처리
    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_common_JJFK0003')& (df['adContent'] == '2301_pc-main_JJGS0014'), 'campaign'] = 'jj_all_br_common_JJFK0004'
    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_cooking_JJFK0003')& (df['adContent'] == '2301_pc-main_JJGS0027'), 'campaign'] = 'jj_all_br_cooking_JJFK0004'

    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_maincategory_JJFK0003')& (df['adContent'] == '2301_pc-main_jjgs0009'), 'campaign'] = 'jj_all_br_maincategory_JJFK0004'
    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_pajama_JJFK0003')& (df['adContent'] == '2301_pc-main_JJGS0019'), 'campaign'] = 'jj_all_br_pajama_JJFK0004'

    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_sitelink-benefit_JJFK0003')& (df['adContent'] == '2301_pc-main_JJGS0035'), 'campaign'] = 'jj_all_br_sitelink-benefit_JJFK0004'
    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_sitelink-event_JJFK0003')& (df['adContent'] == '2301_pc-main_JJGS0033'), 'campaign'] = 'jj_all_br_sitelink-event_JJFK0004'
    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_sitelink-newbest_JJFK0003') & (df['adContent'] == '2301_pc-main_JJGS0034'), 'campaign'] = 'jj_all_br_sitelink-newbest_JJFK0004'
    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_sitelink-sale_JJFK0003') & (df['adContent'] == '2301_pc-main_jjgs0036'), 'campaign'] = 'jj_all_br_sitelink-sale_JJFK0004'
    df.loc[(df['source'] == 'google') & (df['campaign'] == 'jj_all_br_underwear_JJFK0003') & (df['adContent'] == '2301_pc-main_JJGS0021'), 'campaign'] = 'jj_all_br_underwear_JJFK0004'

    df.loc[(df['source'] == 'google') & (df['medium'] == 'cpc') & (df['keyword'] == 'jaju'), 'keyword'] = 'JAJU'
    df.loc[(df['source'] == 'google') & (df['medium'] == 'cpc') & (df['keyword'] == 'jaju'), 'keyword'] = 'JAJU'

    return df

def brand_order():
    df = ga_read('type2',ref.columns.ga2_dtype.keys())

    df = ga_exception(df)
    df = ref.adcode_ga(df)
    df = df.loc[df['머징코드'] != 'None']

    index = ref.br_ind.drop_duplicates(keep= 'last')
    index = index.loc[index['브랜드'] != '-'].drop_duplicates('머징코드')

    merge = pd.merge(df,index, on = '머징코드', how = 'left').fillna('-')

    # 예외처리
    merge.loc[merge['productBrand'] == 'GAP Kids', 'productBrand'] = 'GAP Adults'
    ##
    merge.loc[(merge['머징코드'].isin(['SVFK0123','SVFK0122'])) & (merge['productBrand'].isin(['GIORGIO ARMANI','EMPORIO ARMANI','EMPORIO ARMANI JUNIOR','ARMANI EXCHANGE','EMPORIO ARMANI UNDERWEAR'])), 'productBrand'] = 'GIORGIO ARMANI'

    # 값 수정하기
    order = merge.loc[merge['productBrand'] == merge['브랜드']]
    order = order.sort_values('transactionId')
    order = order.drop_duplicates('transactionId',keep ='first')
    order['브랜드구매(GA)'] = 1

    merge = pd.merge(merge,order, on = ['﻿dataSource', 'browser', 'campaign', 'sourceMedium', 'keyword','adContent', 'transactionId', 'productName', 'productBrand','itemQuantity', 'uniquePurchases', 'itemRevenue', '날짜', 'source','medium', '머징코드', '브랜드'],how = 'left').fillna(0)

    merge['브랜드매출(GA)'] = 0
    merge.loc[merge['productBrand'] == merge['브랜드'],'브랜드매출(GA)'] = merge['itemRevenue']

    merge.to_csv(dr.download_dir + f'GA_raw/ga_raw_{ref.r_date.yearmonth}_브랜드 구매.csv',index = False, encoding = 'utf-8-sig')

    merge = merge[['﻿dataSource', 'browser', 'campaign','keyword','adContent','날짜', 'source', 'medium', '머징코드','브랜드구매(GA)', '브랜드매출(GA)']]
    merge[['sessions', 'users', 'goal1Completions', 'transactions','transactionRevenue']] = 0

    return merge

def ga_report():

    # 여기 수정
    df = ga_read('type1', ref.columns.ga4_rename.keys(), ga4 = '(ga4)',header= 6)
    df = df.rename(columns = ref.columns.ga4_rename)
    df = df.dropna(subset = 'campaign')
    df = ga_exception(df)

    df = df.loc[df['eventname'].isin(['sign_up','session_start','purchase'])]
    ga1 = pd.pivot_table(df ,index = ['날짜','source', 'medium','campaign','adContent', 'keyword','transactions', 'transactionRevenue'], columns= 'eventname' , values= 'eventcount').reset_index().fillna(0)
    ga1 = ref.adcode_ga(ga1)
    ga1 =  ga1.rename(columns=ref.columns.ga4_rename_final)
    ga1[['브랜드구매(GA)','브랜드매출(GA)']] = 0

    user_df = ga_read('type1', ['세션 캠페인', '세션 수동 광고 콘텐츠', '세션 소스/매체', '세션 수동 검색어', '이벤트 이름', '이벤트 수', '총 사용자'], ga4='(ga4user)', header=6)
    user_df = user_df.rename(columns=ref.columns.ga4_rename)
    user_df = user_df.dropna(subset='campaign')
    user_df = user_df.loc[user_df['eventname'].isin(['session_start'])]
    user_df = ga_exception(user_df)
    user_df = ref.adcode_ga(user_df)
    user_df = user_df.loc[user_df['머징코드'] != 'None']
    user_df = user_df.drop(columns = ['sourceMedium','eventname','eventcount']).rename(columns = {'총 사용자':'UA(GA)'})
    br_df = brand_order()
    br_df = br_df.rename(columns=ref.columns.ga1_rename)
    br_df =  br_df[['날짜','source', 'medium', 'campaign', 'adContent', 'keyword', '머징코드','구매(GA)', '매출(GA)', '세션(GA)', 'UA(GA)', '가입(GA)', '브랜드구매(GA)','브랜드매출(GA)']]

    df = pd.concat([ga1, br_df])
    df = pd.concat([df, user_df]).fillna(0)

    df = df[['날짜','source', 'medium', 'campaign', 'adContent', 'keyword', '머징코드','구매(GA)', '매출(GA)', '세션(GA)', 'UA(GA)', '가입(GA)', '브랜드구매(GA)','브랜드매출(GA)']]
    ga_raw = df.groupby(['날짜', '머징코드'])[['구매(GA)', '매출(GA)', 'UA(GA)','세션(GA)', '가입(GA)', '브랜드구매(GA)','브랜드매출(GA)']].sum().reset_index()

    df.to_csv(dr.download_dir + f'GA_raw/ga_raw_{ref.r_date.yearmonth}.csv',index = False, encoding = 'utf-8-sig')
    ga_raw = ref.date_dt(ga_raw)

    return ga_raw
