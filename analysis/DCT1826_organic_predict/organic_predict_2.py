from setting import directory as dr
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta
from prophet import Prophet
import holidays
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.plot import plot_cross_validation_metric
import itertools
import matplotlib
matplotlib.use('Qt5Agg')


def outlier_remove(data, threshold=1.5):
    q1, q3 = np.percentile(data.y, [25, 74])
    IQR = q3 - q1
    lower_bound = q1 - (threshold * IQR)
    upper_bound = q3 + (threshold * IQR)
    replace_value = round(np.mean(data.y))
    outlier = [x for x in model_data.y if x < lower_bound or x > upper_bound]
    data.loc[data['y'].isin(outlier), 'y'] = replace_value
    return data


def get_model_data():
    # 오가닉 학습 데이터
    result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM'
    file_name = '/organic.csv'
    data = pd.read_csv(result_dir + file_name)
    data['date'] = pd.to_datetime(data['date'])  # 날짜 열을 날짜 형식으로 변환
    data = data.loc[data['date'] >= '2022-03-01']
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

    season_df = pd.DataFrame({'ds': data.ds})
    season_df['season'] = 0
    season_month = [3, 4, 5, 6, 9, 10, 11]
    season_df.loc[season_df['ds'].dt.month.isin(season_month), 'season'] = 1
    data = data.merge(season_df, on='ds', how='left')

    model_data = data[['ds', 'y', 'promotion', 'season']]

    return model_data


# 휴일 정보 생성
# date_list = pd.date_range('2022-02-17', '2023-09-30')  # 데이터 기간에 맞게 수정
# kr_holidays = holidays.KR()
# holiday_df = pd.DataFrame(columns=['ds', 'holiday'])
# holiday_df['ds'] = sorted(date_list)
# holiday_df['holiday'] = holiday_df['ds'].apply(lambda x: kr_holidays.get(x) if x in kr_holidays else 'non-holiday')


def get_proper_params(tmp_data):
    search_space = {
        'changepoint_prior_scale': [0.05, 0.1, 0.5, 1.0],
        'seasonality_prior_scale': [0.05, 0.1, 1.0],
        'seasonality_mode': ['additive', 'multiplicative'],
        'weekly_seasonality': [10, 20, 30, 40],
        'daily_seasonality': [10, 20, 30, 40]
    }
    param_combined = [dict(zip(search_space.keys(), v)) for v in itertools.product(*search_space.values())]
    mapes = []
    for param in param_combined:
       print('params', param)
       _m = Prophet(**param)
       _m.add_regressor('season')
       _m.add_seasonality(name='monthly', period=30.5, fourier_order=5)
       _m.fit(tmp_data)
       _cv_df = cross_validation(_m, initial='400 days', period='180 days', horizon='60 days', parallel=None)
       _df_p = performance_metrics(_cv_df, rolling_window=1)
       mapes.append(_df_p['mape'].values[0])

    tuning_results = pd.DataFrame(param_combined)
    tuning_results['mapes'] = mapes
    tuning_results.sort_values('mapes', inplace=True, ignore_index=True)

    params = dict(tuning_results.iloc[0, :-1])

    return params


def get_predicted_install(params, tmp_data, year, month, season_month):
    # Prophet 객체 생성
    m = Prophet(**params)
    m.add_regressor('season')
    m.add_regressor('promotion')
    m.add_seasonality(name='monthly', period=30.5, fourier_order=5)
    # tmp_data = tmp_data.loc[(tmp_data['ds'].dt.year <= year) & (tmp_data['ds'].dt.month < month)]
    m.fit(tmp_data)

    # predict_days = ((tmp_data.ds.max() + relativedelta(months=2)).replace(day=1) - relativedelta(days=1)).day
    predict_days = 60
    future = m.make_future_dataframe(periods=predict_days, freq='d')
    future['season'] = 0
    future.loc[future['ds'].dt.month.isin(season_month), 'season'] = 1
    future['promotion'] = tmp_data['promotion']
    future['promotion'] = future['promotion'].fillna(0)
    forecast = m.predict(future)
    # fig1 = m.plot(forecast)
    # fig2 = m.plot_components(forecast)

    # 모델 평가 지표
    # initial은 전체 기간의 70~80%, period는 패턴 주기에 따라 조정 (분기별 설정), horizon은 예측기간
    df_cv = cross_validation(m, initial='365 days', period='180 days', horizon='60 days')
    df_pm = performance_metrics(df_cv)
    # fig3 = plot_cross_validation_metric(df_cv, metric='mape')
    model_eval = df_pm.iloc[len(df_pm)-1]

    # 모델이 우수한 성능으로 평가되는 지표
    # rmse: 0.5 이하
    # mae: 0.1 미만
    # mape: 0.1 미만

    predicted_install = round(forecast.loc[(forecast['ds'].dt.year == year) & (forecast['ds'].dt.month == month), 'yhat'].sum())

    return model_eval, predicted_install


model_data = get_model_data()
model_data.ds.sort_values()
params = get_proper_params(model_data)
# params = {'changepoint_prior_scale': 0.1, 'seasonality_prior_scale': 0.05, 'seasonality_mode': 'multiplicative', 'weekly_seasonality': 10, 'daily_seasonality': 40}
season_month = [3, 4, 5, 6, 9, 10, 11]
# 2023년 12월 install 예측치 총량
eval_4, install_4 = get_predicted_install(params, model_data, 2023, 4, season_month)
eval_5, install_5 = get_predicted_install(params, model_data, 2023, 5, season_month)
eval_6, install_6 = get_predicted_install(params, model_data, 2023, 6, season_month)
eval_7, install_7 = get_predicted_install(params, model_data, 2023, 7, season_month)
eval_8, install_8 = get_predicted_install(params, model_data, 2023, 8, season_month)
eval_9, install_9 = get_predicted_install(params, model_data, 2023, 9, season_month)
eval_10, install_10 = get_predicted_install(params, model_data, 2023, 10, season_month)
eval_11, install_11 = get_predicted_install(params, model_data, 2023, 11, season_month)
eval_12, install_12 = get_predicted_install(params, model_data, 2023, 12, season_month)


