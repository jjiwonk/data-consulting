import itertools

from setting import directory as dr
import pandas as pd
from prophet import Prophet
import holidays
from prophet.plot import add_changepoints_to_plot


# 결과 파일 경로
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM'
file_name = '/organic.csv'
data = pd.read_csv(result_dir + file_name)

data['date'] = pd.to_datetime(data['date'])  # 날짜 열을 날짜 형식으로 변환
data = data.rename(columns={'date': 'ds', '앱설치': 'y'})

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


'''
#작업 중
search_space = {
    'holidays': [holidays],
    'changepoint_prior_scale': [0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
    'seasonality_prior_scale': [0.05, 0.1, 1.0, 10.0],
    'holidays_prior_scale': [0.05, 0.1, 1.0, 10.0],
    'seasonality_mode': ['additive', 'multiplicative']
}

from prophet.diagnostics import cross_validation, performance_metrics

param_combined = [dict(zip(search_space.keys(), v)) for v in itertools.product(*search_space.values())]

mapes = []
for param in param_combined:
   print('params', param)
   _m = Prophet(**param)

   _m.add_country_holidays(country_name='KR')
   _m.fit(model_data)
   _cv_df = cross_validation(_m, initial='365 days', period='30 days', horizon='92 days', parallel=None)
   _df_p = performance_metrics(_cv_df, rolling_window=1)
   mapes.append(_df_p['mape'].values[0])

tuning_results = pd.DataFrame(param_combined)
tuning_results['mapes'] = mapes

'''