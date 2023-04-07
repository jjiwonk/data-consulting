import pandas as pd
import setting.directory as dr

raw_dir = dr.dropbox_dir + '/데이터컨설팅/데이터 분석 프로젝트/Robyn/raw_data/(라티브)22_23 매출데이터_데이터팀전달용_230309.xlsx'
download_dir = dr.dropbox_dir + '/데이터컨설팅/데이터 분석 프로젝트/Robyn/result_data'
rename_dict = {
    '채널': 'channel',
    '일자': 'date',
    '날짜': 'date',
    '일별': 'date',
    '이벤트 타입': 'event_type',
    '프로모션': 'promotion',
    '최대 할인율': 'max_discount',
    '매출': 'sales',
    '매출구분': 'sales_type',
    '매체': 'media',
    '캠페인유형': 'campaign_type',
    '노출수': 'I',  # impression
    '클릭수': 'C',  # click
    '비용(VAT제외)': 'S',  # spend
}


# 이슈 일정 데이터 가공
def event_schedule_prep():
    event_schedule = pd.read_excel(raw_dir, sheet_name='이슈 일정')
    event_schedule = event_schedule.rename(columns=rename_dict)
    event_schedule['max_discount'] = event_schedule['max_discount'].fillna('')
    event_schedule['event'] = event_schedule.apply(lambda x: (str(x['promotion']) + ' ' + str(x['max_discount'])).rstrip(), axis=1)

    def discount_prep(data):
        if data == '10~15%' or data == 0.15:
            return 0.15
        elif data == '10~17%':
            return 0.17
        elif data == '5%~8%':
            return 0.08
        elif data == 0.05:
            return 0.05
        elif data == 0.2:
            return 0.2
        else:
            return 0
    event_schedule['max_discount'] = event_schedule['max_discount'].apply(lambda x: discount_prep(x))
    event_list = event_schedule['event'].unique().tolist()
    event_dict = dict()
    for i in range(len(event_list)):
        event_dict[event_list[i]] = i + 1
    event_schedule['event_dummy'] = event_schedule['event'].map(event_dict)
    event_df = event_schedule.pivot_table(index='date', columns='event_type', values=['event_dummy', 'max_discount', 'date_num'], aggfunc='sum').reset_index()
    event_df = event_df.fillna(0)
    event_df[('promotion', '')] = event_df[event_df.columns[1:]].sum(axis=1).apply(lambda x: 0 if x == 0 else 1)
    event_df[('max_discount', '')] = event_df[event_df.columns[7:-1]].sum(axis=1)
    event_df = event_df.drop(event_df[event_df.columns[7:-2]], axis=1)
    event_df.columns = ['date', 'under20_date_num', 'over20_date_num', 'non_discount_date_num', 'under20_event', 'over20_event', 'non_discount_event', 'promotion', 'max_discount']
    event_df['date'] = event_df['date'].apply(lambda x: x.strftime("%Y-%m-%d"))
    date_df = pd.DataFrame({'date': [date.strftime("%Y-%m-%d") for date in pd.date_range('2022-01-06', '2023-03-07')]})
    final_event_df = date_df.merge(event_df, how='left', on='date').fillna(0)
    final_event_df[final_event_df.columns[1:-1]] = final_event_df[final_event_df.columns[1:-1]].astype('int')
    weight = 0
    for i in range(len(final_event_df)):
        if final_event_df.loc[i, 'promotion'] > 0:
            final_event_df.loc[i, 'event_weight'] = weight
            weight = 0
        else:
            weight += 1
            final_event_df.loc[i, 'event_weight'] = 0
    final_event_df.loc[final_event_df['date'] == '2022-01-06', 'event_weight'] = 30  # 인수 후 오픈 일자 가중치 plus
    final_event_df['sold_out'] = final_event_df.apply(lambda x: 1 if x['date'] in ['2022-03-05', '2022-03-06', '2022-03-07',
                                                                                   '2022-03-08', '2022-03-09', '2022-03-10',
                                                                                   '2022-09-14', '2022-09-15', '2022-09-16',
                                                                                   '2022-09-17', '2022-09-18'] else 0, axis=1)
    return final_event_df


# 매출 데이터 가공
def sales_data_prep():
    # 자사몰 매출
    company_df = pd.read_excel(raw_dir, sheet_name='RAW_자사몰매출')
    company_df = company_df.rename(columns=rename_dict)
    company_df['date'] = company_df.apply(lambda x: str(x['Year'])+'-'+str(x['Month']).zfill(2)+'-'+str(x['Day']).zfill(2), axis=1)
    company_sales = company_df.pivot_table(index='date', columns='sales_type', values='sales', aggfunc='sum').reset_index()
    company_sales = company_sales.fillna(0)
    company_sales.columns = ['date', 'smartstore_sales', 'company_sales', 'kakaoshopping_sales']
    # 오프라인 매출
    offline_df = pd.read_excel(raw_dir, sheet_name='RAW_오프라인')
    offline_df = offline_df.rename(columns=rename_dict)
    offline_sales = offline_df[['date', 'sales']]
    offline_sales = offline_sales.rename(columns={'sales': 'offline_sales'})
    offline_sales['date'] = offline_sales['date'].astype('str')
    # 유통 매출
    retail_df = pd.read_excel(raw_dir, sheet_name='RAW_유통')
    retail_df = retail_df.rename(columns=rename_dict)
    retail_sales = retail_df.pivot_table(index='date', columns='channel', values='sales', aggfunc='sum').reset_index()
    retail_sales = retail_sales.fillna(0)
    retail_sales.columns = ['date', 'ssgonline_sales', 'oliveyoung_sales', 'kakaogift_sales']
    retail_sales['date'] = retail_sales['date'].astype('str')
    # 전체 통합
    date_df = pd.DataFrame({'date': [date.strftime("%Y-%m-%d") for date in pd.date_range('2022-01-06', '2023-03-07')]})
    sales_df = date_df.merge(company_sales, how='left', on='date')
    sales_df = sales_df.merge(offline_sales, how='left', on='date')
    sales_df = sales_df.merge(retail_sales, how='left', on='date')
    sales_df = sales_df.fillna(0)
    sales_df['total_sales'] = sales_df[sales_df.columns[1:]].sum(axis=1)
    return sales_df


# 미디어 데이터 가공
def media_data_prep(pivot_cols):
    advertise_df = pd.read_excel(raw_dir, sheet_name='RAW_미디어')
    advertise_df = advertise_df.rename(columns=rename_dict)
    campaign_type_dict = {
        'display': 'display',
        'RE': 're',
        'UA': 'ua',
        '검색': 'search',
        '디스커버리': 'discovery',
        '메인': 'main',
        '브랜드검색': '',
        '비즈보드': 'bizboard',
        '쇼핑검색': 'shopping',
        '쇼핑검색-쇼핑브랜드형': 'shopping_brand',
        '스마트채널': 'smartchannel',
        '채널 메시지': 'channel',
        '파워 링크': 'powerlink',
        '파워컨텐츠': 'powercontents'
    }
    advertise_df.campaign_type = advertise_df.campaign_type.apply(lambda x: campaign_type_dict[x])
    result_df = advertise_df.pivot_table(index='date', columns=pivot_cols, values=['I', 'C', 'S'], aggfunc='sum')
    result_df = result_df.fillna(0)
    if 'campaign_type' in pivot_cols:
        result_df.columns = [channel+'_'+campaign_type+'_'+value for value, channel, campaign_type in result_df.columns]
    else:
        result_df.columns = [channel+'_'+value for value, channel in result_df.columns]
    result_df = result_df.reset_index()
    result_df['date'] = result_df['date'].astype('str')
    return result_df


# 전체 데이터 머징
event_df = event_schedule_prep()
sales_df = sales_data_prep()
ad_camp_df = media_data_prep(['media', 'campaign_type'])
ad_media_df = media_data_prep(['media'])
total_df = sales_df.merge(event_df, how='left', on='date')
total_camp_df = total_df.merge(ad_camp_df, how='left', on='date').fillna(0)
total_media_df = total_df.merge(ad_media_df, how='left', on='date').fillna(0)
total_media_df['naver_bs'] = total_media_df['naver_bs_S'].apply(lambda x: 1 if x > 0 else 0)

# 데이터 추출
total_camp_df.to_csv(download_dir + '/라티브_캠페인_유형별_Robyn_실습데이터.csv', encoding='utf-8-sig', index=False)
total_media_df.to_csv(download_dir + '/라티브_매체별_Robyn_실습데이터.csv', encoding='utf-8-sig', index=False)