import itertools

from setting import directory as dr
import pandas as pd
from prophet import Prophet
import holidays
from prophet.plot import add_changepoints_to_plot


# 결과 파일 경로
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM'
organic_file_name = '/organic.csv'
organic_data = pd.read_csv(result_dir + organic_file_name)
paid_file_name = '/DCT1747/prep_data.csv'
paid_data = pd.read_csv(result_dir + paid_file_name)

organic_data['date'] = pd.to_datetime(organic_data['date'])  # 날짜 열을 날짜 형식으로 변환
organic_data['month'] = organic_data['date'].dt.month
organic_data['year'] = organic_data['date'].dt.year
organic_grouped_data = organic_data.groupby(['year', 'month']).agg({'앱설치': 'sum', '첫구매': 'sum'}).reset_index()
organic_grouped_data.to_csv('organic_grouped_data.csv', encoding='cp949')

paid_data['date'] = pd.to_datetime(paid_data['date'])  # 날짜 열을 날짜 형식으로 변환
paid_data['month'] = paid_data['date'].dt.month
paid_data['year'] = paid_data['date'].dt.year
paid_grouped_data = paid_data.groupby(['year', 'month']).agg({'install': 'sum', 'first_purchase': 'sum'}).reset_index()
paid_data.to_csv('paid_data.csv', encoding='cp949')

data = organic_data.rename(columns={'date': 'ds', '앱설치': 'y'})

model_data = data[['ds', 'y']]

'''
### 기본 모델 구현

m = Prophet()
m.fit(model_data)
future = m.make_future_dataframe(periods=92)
forecast = m.predict(future)
fig1 = m.plot(forecast)
fig2 = m.plot_components(forecast)
'''

### 휴일 정보 생성
date_list = pd.date_range('2022-02-17', '2023-09-30')  # 데이터 기간에 맞게 수정
kr_holidays = holidays.KR()
holiday_df = pd.DataFrame(columns=['ds', 'holiday'])
holiday_df['ds'] = date_list
holiday_df['holiday'] = holiday_df['ds'].apply(lambda x: kr_holidays.get(x) if x in kr_holidays else 'non-holiday')


'''
# 모델 실행
m = Prophet(holidays=holiday_df)
m.fit(model_data)

###모델 실행 및 그래프 출력
forecast = m.predict(future)
fig1 = m.plot(forecast)
fig2 = m.plot_components(forecast)
'''

### 프로모션 데이터 추가
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM'
file_name = '/promotion.csv'
promotion_df = pd.read_csv(result_dir + file_name)
# '프로모션 진행 여부' 열의 값을 숫자로 변환
promotion_df['프로모션 진행 여부'] = (promotion_df['프로모션 진행 여부'] == 'O').astype(int)
promotion_df = promotion_df.rename(columns={'날짜':'ds','프로모션 진행 여부': 'promotion'})
promotion_df['ds'] = pd.to_datetime(promotion_df['ds'])  # 날짜 열을 날짜 형식으로 변환
promotion_df_1 = promotion_df[promotion_df['promotion'] == 1]

promotion = pd.DataFrame({
  'holiday': 'promotion',
  'ds': promotion_df_1['ds'],
  'lower_window': 0,
  'upper_window': 1,
})
season = pd.DataFrame({
    'holiday': 'season',
    'ds': pd.concat([
        pd.Series(pd.date_range('2022-03-01', '2022-05-31', freq='D')),
        pd.Series(pd.date_range('2022-09-01', '2022-11-30', freq='D')),
        pd.Series(pd.date_range('2023-03-01', '2023-05-31', freq='D')),
        pd.Series(pd.date_range('2023-09-01', '2023-11-30', freq='D'))
    ])
    # lower_window = 0,
    # upper_window = 1
})

holidays=pd.concat([promotion, season])

# 모델 실행 및 그래프 출력
m = Prophet(holidays=holidays)
m.add_country_holidays(country_name='KR')
m.fit(model_data)

future = m.make_future_dataframe(periods=92)
forecast = m.predict(future)
fig1 = m.plot(forecast)
fig2 = m.plot_components(forecast)

### 다른 요소 넣어서 피팅

# 모델 실행 및 그래프 출력
m = Prophet(
    holidays=holidays,
    holidays_prior_scale=15,
    # trend
    changepoint_prior_scale=0.3,
    # seasonality
    weekly_seasonality=10, #10 (default)
    # yearly_seasonality=10, #10 (default) #year을 추가하면 급격히 시뮬레이션 트렌트가 우하향하는 현상이 보여서 제외함
    daily_seasonality=10,
    # seasonality_mode = 'multiplicative'
)
m.add_country_holidays(country_name='KR')
m.add_seasonality(name='monthly', period=30.5, fourier_order=5)
m.fit(model_data)

future = m.make_future_dataframe(periods=92) #monthly 예측 값 추가 -> freq='MS'
forecast = m.predict(future)
fig1 = m.plot(forecast)
fig2 = m.plot_components(forecast)



def get_model_data():
    # 오가닉 학습 데이터
    result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM'
    file_name = '/organic.csv'
    data = pd.read_csv(result_dir + file_name)
    data['date'] = pd.to_datetime(data['date'])  # 날짜 열을 날짜 형식으로 변환
    data = data.loc[data['date'] >= '2022-03-01']
    # data = data.loc[data['date'] <= '2023-09-30']
    data = data.rename(columns={'date': 'ds', '앱설치': 'y'})


    # 프로모션 진행 여부 데이터
    file_name = '/promotion.csv'
    promotion_df = pd.read_csv(result_dir + file_name)
    # '프로모션 진행 여부' 열의 값을 숫자로 변환
    promotion_df['프로모션 진행 여부'] = (promotion_df['프로모션 진행 여부'] == 'O').astype(int)
    promotion_df = promotion_df.rename(columns={'날짜': 'ds', '프로모션 진행 여부': 'promotion'})
    promotion_df['ds'] = pd.to_datetime(promotion_df['ds'])  # 날짜 열을 날짜 형식으로 변환
    data = data.merge(promotion_df, on='ds', how='left')
    data = data.fillna(0)

    # 성수기, 비수기 구분
    peak_df = pd.DataFrame({'ds': data.ds})
    peak_df['peak'] = 0
    peak_month = [3, 4, 5, 6, 9, 10, 11]
    peak_df.loc[peak_df['ds'].dt.month.isin(peak_month), 'peak'] = 1
    data = data.merge(peak_df, on='ds', how='left')
    print(peak_df.head())

    # 계절 구분 (봄, 여름, 가을, 겨울)
    season_df = pd.DataFrame({'ds': data.ds})
    season_df['season'] = 0
    season_month_spring = [3, 4, 5]
    season_month_summer = [6, 7, 8]
    season_month_fall = [9, 10]
    season_month_winter = [11, 12, 1, 2]
    season_df.loc[season_df['ds'].dt.month.isin(season_month_spring), 'season'] = 1
    season_df.loc[season_df['ds'].dt.month.isin(season_month_summer), 'season'] = 2
    season_df.loc[season_df['ds'].dt.month.isin(season_month_fall), 'season'] = 3
    season_df.loc[season_df['ds'].dt.month.isin(season_month_winter), 'season'] = 4
    data = data.merge(season_df, on='ds', how='left')

    # # 휴일 정보 생성
    # date_list = pd.date_range('2022-03-01', '2023-12-31')
    # kr_holidays = holidays.KR()
    # holiday_df = pd.DataFrame(columns=['ds', 'holiday'])
    # holiday_df['ds'] = sorted(date_list)
    # holiday_df['holiday'] = holiday_df['ds'].apply(lambda x: kr_holidays.get(x) if x in kr_holidays else None)
    #
    # # 휴일 데이터프레임을 모델 데이터에 추가
    # data = data.merge(holiday_df, on='ds', how='left')
    # data['holiday'] = data['holiday'].apply(lambda x: 1 if x is not None else 0)

    # # 네이버 검색량 추가
    # search_file =  '/naver_search.csv'
    # search_df = pd.read_csv(result_dir + search_file)
    # search_df = search_df.rename(columns={'날짜':'ds', '검색량':'search'})
    # search_df['ds'] = pd.to_datetime(search_df['ds'])

    # data = data.merge(search_df, on='ds', how='left')

    model_data = data[['ds', 'y', 'promotion', 'peak', 'season']]

    return model_data



model_data = get_model_data()
m = Prophet(
    changepoint_prior_scale=0.1,
    seasonality_prior_scale=0.1,
    seasonality_mode='multiplicative',
    # holidays_prior_scale=0.1
)
# m.add_country_holidays(country_name='KR')
m.add_regressor('promotion')
m.add_regressor('peak')
m.add_regressor('season')
# m.add_regressor('search')
m.fit(model_data)

future = m.make_future_dataframe(periods=61)
future['promotion'] = model_data['promotion']
future['peak'] = model_data['peak']
peak_month = [3, 4, 5, 6, 9, 10, 11]
future.loc[future['ds'].dt.month.isin(peak_month), 'peak'] = 1

future['season'] = model_data['season']
season_month_spring = [3, 4, 5]
season_month_summer = [6, 7, 8]
season_month_fall = [9, 10]
season_month_winter = [11, 12, 1, 2]
future.loc[future['ds'].dt.month.isin(season_month_spring), 'season'] = 1
future.loc[future['ds'].dt.month.isin(season_month_summer), 'season'] = 2
future.loc[future['ds'].dt.month.isin(season_month_fall), 'season'] = 3
future.loc[future['ds'].dt.month.isin(season_month_winter), 'season'] = 4

# future['search'] = model_data['search']


future['promotion'] = future['promotion'].fillna(0)
future['peak'] = future['peak'].fillna(0)
# future['search'] = future['search'].fillna(0)

forecast = m.predict(future)
fig = m.plot_components(forecast)

predicted_install_yhat= round(forecast.loc[(forecast['ds'].dt.year == 2023) & (forecast['ds'].dt.month == 11), 'yhat'].sum())
predicted_install_lower = round(forecast.loc[(forecast['ds'].dt.year == 2023) & (forecast['ds'].dt.month == 11), 'yhat_lower'].sum())
predicted_install_upper = round(forecast.loc[(forecast['ds'].dt.year == 2023) & (forecast['ds'].dt.month == 11), 'yhat_upper'].sum())

forecast.columns

search_space = {
    'changepoint_prior_scale': [0.05, 0.1, 0.5, 1.0],
    'seasonality_prior_scale': [0.05, 0.1, 1.0],
    'seasonality_mode': ['additive', 'multiplicative'],
    # 'holidays_prior_scale': [0.05, 0.1, 1.0, 10.0],
    # 'weekly_seasonality': [10, 20, 30, 40],
    # 'daily_seasonality': [10, 20, 30, 40]
}

from prophet.diagnostics import cross_validation, performance_metrics

param_combined = [dict(zip(search_space.keys(), v)) for v in itertools.product(*search_space.values())]

mapes = []
for param in param_combined:
   print('params', param)
   _m = Prophet(**param)

   _m.add_country_holidays(country_name='KR')
   _m.fit(model_data)
   _cv_df = cross_validation(_m, initial='365 days', period='90 days', horizon='30 days')
   _df_p = performance_metrics(_cv_df, rolling_window=1)
   mapes.append(_df_p['mape'].values[0])

tuning_results = pd.DataFrame(param_combined)
tuning_results['mapes'] = mapes
tuning_results.sort_values(by='mapes', ascending=True, inplace=True)
