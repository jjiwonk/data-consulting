from report.SIV import directory as dr
from report.SIV import ref
import pyarrow.csv as pacsv
import pyarrow as pa
import datetime
import pandas as pd

def ga_read():
    dir = dr.report_dir + 'ga_type1_prism'

    from_date = ref.r_date.start_date
    to_date = ref.r_date.target_date

    ga_raw = pd.DataFrame()

    while from_date <= to_date : #리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        file_name = f'127881812_type1_daily_report_{date_name}.csv'

        file = pd.read_csv(dir + '/' + file_name, usecols= ref.columns.ga1_dtype.keys())
        file['날짜'] = from_date.strftime('%Y-%m-%d')
        ga_raw = ga_raw.append(file)

        print(file_name + ' Read 완료')

        from_date = from_date + datetime.timedelta(1)

    return ga_raw

def ga3_read():
    dir = dr.report_dir + 'ga_type3_prism'

    #from_date = ref.r_date.start_date
    from_date = datetime.date(2023, 1, 6)
    to_date = ref.r_date.target_date

    ga_raw = pd.DataFrame()

    while from_date <= to_date:  # 리포팅 데이트가 달 첫날보다 클 경우엔 참 > 즉 첫날부터 리포팅데이트까지 반복
        date_name = from_date.strftime('%Y%m%d')
        file_name = f'127881812_type3_daily_report_{date_name}.csv'

        file = pd.read_csv(dir + '/' + file_name, usecols=ref.columns.ga3_dtype.keys())
        file['날짜'] = from_date.strftime('%Y-%m-%d')
        ga_raw = ga_raw.append(file)

        print(file_name + ' Read 완료')

        from_date = from_date + datetime.timedelta(1)

    pmax_campaign = ['[2023.01]Pmax_Pmax','정기_pmax_sales_2301_Pmax','[2023.01]Pmax_Pmax_SVGG0001','정기_pmax_sales_2301_Pmax_JJGG0015']
    ga_raw = ga_raw.loc[ga_raw['campaign'].isin(pmax_campaign)]

    ga_raw['source'] = 'google'
    ga_raw['medium'] = 'cpc'
    ga_raw[['keyword','adContent']] = '-'

    return ga_raw

def ga_prep():
    df = ga_read()
    df3 = ga3_read()
    df = pd.concat([df, df3])

    df = df.loc[df['source'].isin(ref.columns.ga1_media)]
    df = df.loc[df['﻿dataSource'] == 'web']

    #예외처리(크리테오)
    df.loc[df['medium'] == 'da_app_If', 'medium'] = 'da_app_lf'

    criteo_dc = ['feed','(not set)','siv_banner','jaju_sales']
    criteo_s = ['criteo','criteo_cca','criteo_mo','criteo_pc']

    df.index = range(len(df))

    idx = df[(df['source'].isin(criteo_s))&(df['campaign'].isin(criteo_dc))].index
    idx2 = df[(df['campaign'] == 'dynamic')&(df['medium'] == 'da_app_lf')&(df['browser'] == 'Chrome')].index
    idx3 = df[(df['campaign'] == 'jaju')&(df['medium'] == 'da_cca')&(df['browser'] == 'Chrome')].index
    idx4 = df[(df['campaign'] == 'jaju')&(df['medium'] == 'da_mf')&(df['browser'] == 'Edge')].index

    df.drop(idx, inplace = True)
    df.drop(idx2, inplace = True)
    df.drop(idx3, inplace=True)
    df.drop(idx4, inplace=True)

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
    c_media = ['google','naver']
    df.loc[df['source'].isin(c_media), 'campaign'] = df['campaign'].apply(lambda x: x.replace(x, ref.exc_cdict[x]) if x in ref.exc_cdict.keys() else x)

    df.loc[df['source'] == 'criteo', 'adContent'] = df['adContent'].apply(lambda x: x.replace(x, ref.exc_ga_adict[x]) if x in ref.exc_ga_adict.keys() else x)

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

    # 끝 (위에 코드 붙임)
    df = ref.adcode_ga(df)
    df = df.rename(columns = ref.columns.ga1_rename)
    df[['브랜드구매(GA)', '브랜드매출(GA)']] = 0

    df = df[['머징코드','날짜','﻿dataSource', 'browser', 'campaign', 'source', 'medium', 'keyword','adContent', '세션(GA)', 'UA(GA)', '구매(GA)','매출(GA)','브랜드구매(GA)', '브랜드매출(GA)','가입(GA)']]

    return df

ga = ga_prep()

ga.to_csv(dr.download_dir +'ga_raw.csv', index= False , encoding= 'utf-8-sig')


























