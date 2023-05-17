from report.SIV import directory as dr
from report.SIV import ref
import datetime
import pandas as pd

def ga_read(type,use_col):
    dir = dr.report_dir + f'ga_{type}_prism'

    from_date = ref.r_date.start_date
    to_date = ref.r_date.target_date

    ga_raw = pd.DataFrame()

    while from_date <= to_date : #리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        file_name = f'127881812_{type}_daily_report_{date_name}.csv'

        file = pd.read_csv(dir + '/' + file_name, usecols= use_col)
        file['날짜'] = from_date.strftime('%Y-%m-%d')
        ga_raw = ga_raw.append(file)

        print(file_name + ' Read 완료')

        from_date = from_date + datetime.timedelta(1)

    return ga_raw

def ga3_prep():
    df = ga_read('type3',ref.columns.ga3_dtype.keys())

    pmax_campaign = ['[2023.01]Pmax_Pmax','정기_pmax_sales_2301_Pmax','[2023.01]Pmax_Pmax_SVGG0001','정기_pmax_sales_2301_Pmax_JJGG0015']
    df = df.loc[df['campaign'].isin(pmax_campaign)]

    df['source'] = 'google'
    df['medium'] = 'cpc'
    df[['keyword','adContent']] = '-'
    return df

def ga_exception(df):

    df = df.loc[df['source'].isin(ref.columns.ga1_media)]
    df = df.loc[df['﻿dataSource'] == 'web']
    df['source'].unique()
    #예외처리(크리테오)
    df.loc[df['medium'] == 'da_app_If', 'medium'] = 'da_app_lf'

    criteo_dc = ['feed','(not set)','siv_banner','jaju_sales']
    criteo_s = ['criteo','criteo_cca','criteo_mo','criteo_pc']

    df.index = range(len(df))

    idx = df[(df['source'].isin(criteo_s))&(df['campaign'].isin(criteo_dc))].index
    idx = idx.append(df[(df['campaign'] == 'dynamic')&(df['medium'] == 'da_app_lf')&(df['browser'] == 'Chrome')].index)
    idx = idx.append(df[(df['campaign'] == 'jaju')&(df['medium'] == 'da_cca')&(df['browser'] == 'Chrome')].index)
    idx = idx.append(df[(df['campaign'] == 'jaju')&(df['medium'] == 'da_mf')&(df['browser'] == 'Edge')].index)

    # 브라우저 필터링 더하기
    idx = idx.append(df[(df['source'] == 'criteo') & (df['medium'] == 'catalog') & (df['adContent'] == '2301_sales_catalog_SVCT0001') & (df['browser'].isin(['Android Webview','Safari (in-app)']))].index)
    idx = idx.append(df[(df['source'] == 'adisonofferwall_int') & (df['medium'] == 'display') & (df['browser'].isin(['Android Webview', 'Safari (in-app)']))].index)
    idx = idx.append(df[(df['source'] == 'buzzad_int') & (df['medium'] == 'display') & (df['campaign'] == 'siv_cpa_network') & (df['browser'].isin(['Android Webview', 'Safari (in-app)']))].index)
    df.drop(idx, inplace = True)

    df.loc[(df['campaign'] == 'dynamic')&(df['medium'] == 'da_mo_cca') , 'campaign'] = 'CCA'
    df.loc[(df['campaign'] == 'dynamic')&(df['medium'] == 'da_mo_lf') , 'campaign'] = 'LF - MOBILE - CVO'
    df.loc[(df['campaign'] == 'dynamic') & (df['medium'] == 'da_pc_lf'), 'campaign'] = 'LF - DESKTOP - CVO'
    df.loc[(df['campaign'] == 'dynamic') & (df['medium'] == 'da_app_lf')& (df['browser'] == 'Safari'), 'campaign'] = 'LF - IOS'
    df.loc[(df['campaign'] == 'dynamic') & (df['medium'] == 'da_app_lf')& (df['browser'] == 'Samsung Internet'), 'campaign'] = 'LF - ANDROID'

    df.loc[(df['campaign'] == 'jaju') & (df['medium'] == 'da_mf') & (df['browser'] == 'Safari'), 'campaign'] = 'MF - Desktop/Mobile'
    df.loc[(df['campaign'] == 'jaju') & (df['medium'] == 'da_mo_lf') & (df['browser'] == 'Chrome'), 'campaign'] = 'LF - Mobile'
    df.loc[(df['campaign'] == 'jaju') & (df['medium'] == 'da_mo_lf') & (df['browser'] == 'Safari'), 'campaign'] = 'LF - Mobile'
    df.loc[(df['campaign'] == 'jaju') & (df['medium'] == 'da_mo_lf') & (df['browser'] == 'Samsung Internet'), 'campaign'] = 'LF - Mobile'
    df.loc[(df['campaign'] == 'jaju') & (df['medium'] == 'da_pc_lf') & (df['browser'] == 'Chrome'), 'campaign'] = 'LF - Desktop'
    df.loc[(df['campaign'] == 'jaju') & (df['medium'] == 'da_pc_lf') & (df['browser'] == 'Edge'), 'campaign'] = 'LF - Desktop'

    #예외처리(시트 + 브검)
    dic =ref.exc_ga_adict
    c_media = ['google','naver','navershoppingsa']
    df.loc[df['source'].isin(c_media), 'campaign'] = df['campaign'].apply(lambda x: x.replace(x, ref.exc_cdict[x]) if x in ref.exc_cdict.keys() else x)

    a_media = ['criteo','rtbhouse_int']
    df.loc[df['source'].isin(a_media), 'adContent'] = df['adContent'].apply(lambda x: x.replace(x, ref.exc_ga_adict[x]) if x in ref.exc_ga_adict.keys() else x)
    check = df.loc[df['source'] == 'rtbhouse_int']
    df['브검구분'] = df['adContent'].apply(lambda x : x.replace(x, '브랜드') if x.find('브랜드') != -1 else '-')
    df.loc[df['브검구분'] == '-','브검구분'] = df['adContent'].apply(lambda x: x.replace(x, '일반') if x.find('일반') != -1 else '-')

    df.loc[(df['campaign'] == 'jaju') & (df['source'] == 'naver') & (df['medium'] == 'sa_mo') & (df['브검구분'] == '브랜드'), 'campaign'] = 'jaju_JJFK0005'
    df.loc[(df['campaign'] == 'jaju') & (df['source'] == 'naver') & (df['medium'] == 'sa_pc') & (df['브검구분'] == '브랜드'), 'campaign'] = 'jaju_JJFK0006'
    df.loc[(df['campaign'] == 'jaju') & (df['source'] == 'naver') & (df['medium'] == 'sa_mo') & (df['브검구분'] == '일반'), 'campaign'] = 'jaju_JJFK0007'
    df.loc[(df['campaign'] == 'jaju') & (df['source'] == 'naver') & (df['medium'] == 'sa_pc') & (df['브검구분'] == '일반'), 'campaign'] = 'jaju_JJFK0008'

    df = df.drop(columns = '브검구분')

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

    df['source'] = df['sourceMedium'].apply(lambda x: x.split(' / ')[0])
    df['medium'] = df['sourceMedium'].apply(lambda x: x.split(' / ')[-1]).drop(columns='sourceMedium')

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

    df = ga_read('type1', ref.columns.ga1_dtype.keys())
    df3 = ga3_prep()
    df = pd.concat([df, df3])

    df = ga_exception(df)
    df = ref.adcode_ga(df)
    df[['브랜드구매(GA)','브랜드매출(GA)']] = 0

    br_df = brand_order()
    df = pd.concat([df, br_df])

    df = df.rename(columns=ref.columns.ga1_rename)
    df.to_csv(dr.download_dir + f'GA_raw/ga_raw_{ref.r_date.yearmonth}.csv',index = False, encoding = 'utf-8-sig')
    df = ref.date_dt(df)

    return df

