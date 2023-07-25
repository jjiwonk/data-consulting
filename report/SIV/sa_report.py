import pandas as pd
from report.SIV import ref
from report.SIV import directory as dr
import report.SIV.ga_prep as gprep
import report.SIV.media_prep as mprep
import report.SIV.apps_prep as aprep
import re
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

def media_read(day) :

    kakaosa_df = mprep.get_카카오SA_SA()
    kakaobsa_df = mprep.get_카카오BSA_SA()
    googlesa_df = mprep.get_구글SA_SA()
    naversa_df = mprep.get_네이버SA_SA()
    naverbsa_df = mprep.get_NOSP_SA()

    df = pd.concat([kakaosa_df,kakaobsa_df,googlesa_df,naversa_df,naverbsa_df]).fillna(0)
    df = ref.adcode(df,'캠페인','세트','소재')

    metric = ['노출','도달','클릭','비용','조회','SPEND_AGENCY','구매(대시보드)','매출(대시보드)','설치(대시보드)']
    df[metric] = df[metric].astype(float)
    df['sum'] = df[metric].sum(axis=1)
    df = df.loc[df['sum'] >= 1].drop(columns = ['sum','소재'])

    df = ref.week_day(df)
    df['키워드'] = df['키워드'].apply(lambda x : x.lower())

    dimension = ['머징코드','캠페인', '세트', '키워드','연도','월'] + day
    metric = ['노출', '도달', '클릭', '조회','비용', 'SPEND_AGENCY']
    df = df.groupby(dimension)[metric].sum().reset_index()

    df.to_csv(dr.download_dir + f'keyword_raw/keyword_media_raw_{ref.r_date.yearmonth}_{day}.csv', index=False,encoding='utf-8-sig')

    return df

def ga_read(day):

    df = gprep.ga_read('type1', ga4='(ga4)', header=6)
    df.columns = ['세션 캠페인', '세션 소스/매체', '세션 수동 광고 콘텐츠', '세션 수동 검색어', '이벤트 이름', '세션수', '이벤트 수', '구매 수익','거래', '총 합계','날짜']
    df = df.rename(columns=ref.columns.ga4_rename)
    df = df.dropna(subset='campaign')
    df = gprep.ga_exception(df)

    df = df.loc[(df['medium'] == 'cpc')|(df['medium'] == 'bsa')]
    df = df.loc[df['campaign']!='[2023.01]Pmax_Pmax_SVGG0001']

    df = ref.week_day(df)
    df['keyword'] = df['keyword'].apply(lambda x: x.lower())
    df = df.loc[df['eventname'].isin(['sign_up','session_start','purchase'])]

    piv = pd.pivot_table(df ,index = ['날짜','source', 'medium','campaign','adContent', 'keyword','transactions', 'transactionRevenue'], columns= 'eventname' , values= 'eventcount').reset_index().fillna(0)
    piv = ref.adcode_ga(piv)
    piv =  piv.rename(columns=ref.columns.ga4_rename_final)

    user_df = gprep.ga_read('type1', ga4='(ga4user)', header=6)
    user_df.columns = ['세션 캠페인', '세션 소스/매체', '세션 수동 광고 콘텐츠', '세션 수동 검색어', '이벤트 이름', '세션수', '이벤트 수', '총 사용자','총 합계','날짜']
    user_df = user_df.rename(columns=ref.columns.ga4_rename)
    user_df = user_df.dropna(subset='campaign')
    user_df = user_df.loc[user_df['eventname'].isin(['session_start'])]
    user_df = gprep.ga_exception(user_df)

    user_df = user_df.loc[(user_df['medium'] == 'cpc') | (user_df['medium'] == 'bsa')]
    user_df = user_df.loc[user_df['campaign'] != '[2023.01]Pmax_Pmax_SVGG0001']

    user_df = ref.adcode_ga(user_df)
    user_df = user_df.loc[user_df['머징코드'] != 'None']
    user_df = user_df.drop(columns = ['sourceMedium','eventname','eventcount','총 합계']).rename(columns = {'총 사용자':'UA(GA)'})
    user_df['key'] = user_df['머징코드'] + user_df['keyword']
    user_df = user_df[['날짜', 'campaign', 'adContent', 'keyword', 'UA(GA)', 'source','medium', '머징코드']]
    user_df = ref.date_dt(user_df)

    ga_raw = pd.concat([piv,user_df]).fillna(0)
    ga_raw = ref.week_day(ga_raw)
    ga_raw[['브랜드구매(GA)', '브랜드매출(GA)']] = 0

    dimension = ['머징코드', 'keyword','연도', '월'] + day
    metric = ['세션(GA)', 'UA(GA)', '구매(GA)', '매출(GA)', '가입(GA)','브랜드구매(GA)','브랜드매출(GA)']
    ga_raw = ga_raw.groupby(dimension)[metric].sum().reset_index().rename(columns={'keyword': '키워드'})

    ga_raw.to_csv(dr.download_dir + f'keyword_raw/keyword_ga_raw_{ref.r_date.yearmonth}_{day}.csv', index=False, encoding='utf-8-sig')

    return ga_raw

def apps_read(day):

    df = aprep.apps_concat()
    df = ref.week_day(df)
    df['키워드'] = df['키워드'].apply(lambda x: x.lower())

    dimension = ['연도', '월','머징코드','키워드'] + day
    metric = ['유입(AF)', 'UV(AF)', 'appopen(AF)','구매(AF)', '매출(AF)', '주문취소(AF)', '주문취소매출(AF)', '총주문건(AF)', '총매출(AF)','브랜드구매(AF)', '브랜드매출(AF)', '첫구매(AF)', '첫구매매출(AF)', '설치(AF)', '재설치(AF)','가입(AF)']
    df = df.groupby(dimension)[metric].sum().reset_index()

    df.to_csv(dr.download_dir + f'keyword_raw/keyword_apps_raw_{ref.r_date.yearmonth}_{day}.csv', index=False, encoding='utf-8-sig')

    return df

def sa_merging(day):

    media_df = media_read(day)
    ga_df = ga_read(day)
    apps = apps_read(day)

    ga_dimension = ['머징코드','키워드','연도', '월'] + day
    ga_metric = ['세션(GA)', 'UA(GA)', '구매(GA)', '매출(GA)','브랜드구매(GA)', '브랜드매출(GA)', '가입(GA)']

    ga_df.loc[ga_df['키워드'].isin(['{keyword}', '{query}']), '키워드'] = '(not set)'
    ga_df = ga_df.groupby(ga_dimension)[ga_metric].sum().reset_index().rename(columns ={'keyword' : '키워드'})

    # 인덱스 (다음달에 raw 추가하기)
    merge_index = media_df[['머징코드', '캠페인', '세트', '키워드']]
    merge_index2 = pd.read_csv(dr.download_dir + f'keyword_raw/keyword_media_raw_{ref.r_date.index_date}_{day}.csv')
    merge_index2 = merge_index2[['머징코드', '캠페인', '세트', '키워드']]
    merge_index = pd.concat([merge_index,merge_index2])

    merge_index = merge_index[['머징코드', '캠페인', '세트','키워드']].drop_duplicates(keep='last')
    merge_index['key'] = merge_index['머징코드'] + merge_index['키워드']
    merge_index = merge_index.loc[merge_index['머징코드'] != 'None']
    merge_index = merge_index.drop_duplicates(subset='key',keep='last')

    #last 기준으로 없애기
    merge_cdict = dict(zip(merge_index['key'], merge_index['캠페인']))
    merge_gdict = dict(zip(merge_index['key'], merge_index['세트']))
    merge_ndict = dict(zip(merge_index['머징코드'], merge_index['캠페인']))

    media_df = media_df.loc[media_df['머징코드'] != 'None']
    ga_df = ga_df.loc[ga_df['머징코드'] != 'None']

    media_df['key'] = media_df['머징코드'] + media_df['키워드']
    ga_df['key'] = ga_df['머징코드'] + ga_df['키워드']
    apps['key'] = apps['머징코드'] + apps['키워드']

    metric = ga_dimension + ['key']

    #ga 예외처리
    ga_notset = ga_df.loc[ga_df['키워드'] =='(not set)']
    notset_merge_df = pd.merge(media_df, ga_notset, how='outer', on= metric).fillna(0)
    notset_merge_df = notset_merge_df.loc[notset_merge_df['키워드'] =='(not set)']
    notset_merge_df.loc[notset_merge_df['캠페인'] == 0, '캠페인'] = notset_merge_df['머징코드'].apply(lambda x: x.replace(x, merge_ndict[x]) if x in merge_ndict.keys() else '-')
    notset_merge_df = notset_merge_df.loc[notset_merge_df['캠페인'] != '-']
    notset_merge_df['세트'] = '-'

    notset_merge_df = notset_merge_df.loc[notset_merge_df['주차'] >= 3]

    # 본래 ga 코드
    ga_df = ga_df.loc[ga_df['키워드'] !='(not set)']

    merge_df = pd.merge(media_df, ga_df, how='outer', on= metric).fillna(0)

    merge_df.loc[merge_df['캠페인'] == 0,'캠페인'] = merge_df['key'].apply(lambda x: x.replace(x, merge_cdict[x]) if x in merge_cdict.keys() else '-')
    merge_df.loc[merge_df['세트'] == 0, '세트'] = merge_df['key'].apply(lambda x: x.replace(x, merge_gdict[x]) if x in merge_gdict.keys() else '-')

    merge_df = merge_df.loc[merge_df['캠페인'] != '-']
    merge_df = pd.concat([merge_df, notset_merge_df])

    # 앱스 머징
    merge_df = pd.merge(merge_df, apps, how='outer', on=metric).fillna(0)

    merge_df.loc[merge_df['캠페인'] == 0, '캠페인'] = merge_df['key'].apply(lambda x: x.replace(x, merge_cdict[x]) if x in merge_cdict.keys() else '-')
    merge_df.loc[merge_df['세트'] == 0, '세트'] = merge_df['key'].apply(lambda x: x.replace(x, merge_gdict[x]) if x in merge_gdict.keys() else '-')

    merge_df = merge_df.loc[merge_df['캠페인'] != '-']

    return merge_df

def shopping_sa(day):

    naverss_df = mprep.get_네이버SS_SA()
    naverss_df = ref.adcode(naverss_df, '캠페인', '세트', '소재')
    naverss_df['날짜'] = pd.to_datetime(naverss_df['날짜'])

    naverss_df = ref.week_day(naverss_df)

    dimension = ['머징코드','캠페인', '세트', '키워드','연도','월'] + day
    metric = ['노출','클릭','비용', 'SPEND_AGENCY']

    naverss_df = naverss_df.groupby(dimension)[metric].sum().reset_index()

    ga_df = gprep.ga_read('type1', ga4='(ga4)', header=6)
    ga_df.columns = ['세션 캠페인', '세션 소스/매체', '세션 수동 광고 콘텐츠', '세션 수동 검색어', '이벤트 이름', '세션수', '이벤트 수', '구매 수익','거래', '총 합계','날짜']

    ga_user_df = gprep.ga_read('type1', ga4='(ga4user)', header=6)
    ga_user_df.columns = ['세션 캠페인', '세션 소스/매체', '세션 수동 광고 콘텐츠', '세션 수동 검색어', '이벤트 이름', '세션수', '이벤트 수', '총 사용자','총 합계','날짜']

    def data_prep(ga_df) :
        ga_df = ga_df.dropna(subset='세션 캠페인')
        ga_df = ga_df.rename(columns=ref.columns.ga4_rename)
        ga_df['source'] = ga_df['sourceMedium'].apply(lambda x: x.split(' / ')[0])
        ga_df['medium'] = ga_df['sourceMedium'].apply(lambda x: x.split(' / ')[-1]).drop(columns='sourceMedium')

        ga_df = ga_df.loc[ga_df['source'] == 'navershoppingsa']

        # last 기준으로 없애기
        shopping_cdict = dict(zip(ref.shoppingsa_index['그룹ID'], ref.shoppingsa_index['캠페인명']))
        shopping_gdict = dict(zip(ref.shoppingsa_index['그룹ID'], ref.shoppingsa_index['그룹명']))

        ga_df[['세트']] = ga_df[['campaign']]
        ga_df.loc[ga_df['source'] == 'navershoppingsa', '세트'] = ga_df['campaign'].apply(
            lambda x: x.replace(x, shopping_gdict[x]) if x in shopping_gdict.keys() else '-')
        ga_df.loc[ga_df['source'] == 'navershoppingsa', 'campaign'] = ga_df['campaign'].apply(
            lambda x: x.replace(x, shopping_cdict[x]) if x in shopping_cdict.keys() else '-')

        ga_df = ga_df.loc[ga_df['세트'] != '-']
        ga_df = ref.adcode_ga(ga_df)

        return ga_df

    ga_df = data_prep(ga_df)
    ga_user_df = data_prep(ga_user_df)

    ga_user_df.loc[ga_user_df['eventname'] =='session_start','eventname'] = 'user'
    ga_user_df = ga_user_df.loc[ga_user_df['eventname'] == 'user']
    ga_user_df = pd.pivot_table(ga_user_df,index=['날짜', 'source', 'medium', 'campaign', '세트', 'adContent', 'keyword'], columns='eventname', values='총 사용자').reset_index().fillna(0)

    ga_df = ga_df.loc[ga_df['eventname'].isin(['sign_up', 'session_start', 'purchase'])]
    ga_df = pd.pivot_table(ga_df, index=['날짜', 'source', 'medium', 'campaign','세트', 'adContent', 'keyword', 'transactions','transactionRevenue'], columns='eventname',values='eventcount').reset_index().fillna(0)

    ga_df = pd.concat([ga_df,ga_user_df]).fillna(0)
    ga_df = ref.adcode_ga(ga_df)
    ga_df = ga_df.rename(columns=ref.columns.ga4_rename_final)

    ga_df = ga_df.rename(columns={'sessions': '세션(GA)','user': 'UA(GA)','transactions': '구매(GA)','transactionRevenue': '매출(GA)','goal1Completions': '가입(GA)'
                                  ,'campaign': '캠페인','keyword': '키워드'})

    ga_df = ref.week_day(ga_df)

    ga_dimension = ['머징코드','캠페인','세트','키워드','연도', '월'] + day
    ga_metric = ['세션(GA)', 'UA(GA)', '구매(GA)', '매출(GA)', '가입(GA)']
    ga_df.loc[ga_df['키워드'].isin(['{keyword}', '{query}']), '키워드'] = '(not set)'

    ga_df = ga_df.groupby(ga_dimension)[ga_metric].sum().reset_index().rename(columns ={'keyword' : '키워드'})

    df = pd.concat([naverss_df,ga_df]).fillna(0)
    df.to_csv(dr.download_dir + f'keyword_raw/naverss_report_{ref.r_date.yearmonth}_{day}.csv', index=False,encoding='utf-8-sig')

    return df

def indexing(day):

    merge_df = sa_merging(day)
    shopping_df = shopping_sa(day)

    df = pd.concat([merge_df, shopping_df]).fillna(0)

    index = ref.index_df[['지면/상품','매체','캠페인 구분','KPI','캠페인 라벨','OS','파트(주체)','파트 구분','머징코드']].drop_duplicates()
    index['중복'] = index.duplicated(['머징코드'])
    index = index.loc[index['중복'] == False]
    df = pd.merge(df, index, on='머징코드', how='left').fillna('no_index')

    #키워드 구분 컬럼 추가
    df['키워드 구분'] = '브랜드KW'

    keyword_dict = dict(zip(ref.shoppingsa_index['키워드구분세트'], ref.shoppingsa_index['키워드구분']))
    df.loc[(df['캠페인 구분'] == 'SA') & (df['매체'].isin(['네이버SA', '카카오SA', '구글SA']))&(df['지면/상품'].isin(['검색_M', '검색_P'])), '키워드 구분'] = df['세트'].apply(lambda x: x.replace(x, keyword_dict[x]) if x in keyword_dict.keys() else '브랜드KW')

    df.loc[(df['파트 구분'] == 'SIVILLAGE') & (df['캠페인 구분'] == 'SA') & (df['매체'] == '네이버SA') & (df['지면/상품'].isin(['쇼핑검색_M', '쇼핑검색_P'])), '키워드 구분'] = '쇼핑검색'
    df.loc[(df['파트 구분'] == 'SIVILLAGE') & (df['캠페인 구분'] == 'BSA') & (df['매체'].isin(['네이버BSA', '카카오BSA'])) & (df['지면/상품'].isin(['네이버BSA_M', '네이버BSA_P', '카카오BSA_P'])),'키워드 구분'] = 'BSA'
    df.loc[(df['파트 구분'] == 'SIVILLAGE') & (df['캠페인 구분'] == 'SA') & (df['매체'].isin(['네이버SA', '카카오SA', '구글SA'])) & (df['세트'].isin(ref.shoppingsa_index['일반키워드 세트'])), '키워드 구분'] = '일반KW'
    df.loc[(df['파트 구분'] == 'SIVILLAGE') & (df['캠페인 구분'] == 'SA') & (df['매체'].isin(['네이버SA', '카카오SA', '구글SA'])) & (df['세트'].isin(ref.shoppingsa_index['메인키워드 세트'])), '키워드 구분'] = '메인KW'

    report_col = ['키워드 구분','파트 구분', '연도', '월'] + day +[ '매체', '지면/상품', '캠페인 구분', 'KPI', '캠페인', '세트', '키워드', '캠페인 라벨', 'OS', '노출', '도달', '클릭', '조회','비용', 'SPEND_AGENCY','세션(GA)', 'UA(GA)', '구매(GA)', '매출(GA)',
       '브랜드구매(GA)', '브랜드매출(GA)', '가입(GA)','유입(AF)', 'UV(AF)', 'appopen(AF)','구매(AF)', '매출(AF)', '주문취소(AF)', '주문취소매출(AF)', '총주문건(AF)', '총매출(AF)', '첫구매(AF)', '첫구매매출(AF)', '설치(AF)', '재설치(AF)','가입(AF)','브랜드구매(AF)','브랜드매출(AF)']

    df = df[report_col]
    df[['브랜드구매(GA)', '브랜드매출(GA)','브랜드구매(AF)','브랜드매출(AF)']] = 0

    df.to_csv(dr.download_dir + f'keyword_report/keyword_report_{ref.r_date.yearmonth}_{day}.csv', index=False,encoding='utf-8-sig')

    print('SA/BSA 리포트 추출 완료')

    return df

week_df = indexing(['주차'])
day_df = indexing(['주차','날짜'])
