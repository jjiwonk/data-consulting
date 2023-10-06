import pandas as pd
import matplotlib.pyplot as plt
import statsmodels.api as sm
from setting import directory as dr

# 데이터 로드 함수
def load_data(file_path):
    return pd.read_csv(file_path)


# 함수형 회귀 분석 코드
def perform_regression_analysis(data, num_bins=None, media=None, install_date_time=None):

    # 데이터프레임에서 "label" 열을 더미 변수로 변환
    data = pd.get_dummies(data, columns=['label'], prefix='label')
    data = data.groupby(
        ['install_date', 'media', 'campaign', 'label_첫구매', 'label_구분불가', 'label_인스톨']).sum().reset_index()

    data['label_첫구매'] = data['label_첫구매'].apply(lambda x: 1 if x == True else 0)
    data['label_구분불가'] = data['label_구분불가'].apply(lambda x: 1 if x == True else 0)
    data['label_인스톨'] = data['label_인스톨'].apply(lambda x: 1 if x == True else 0)

    if media is not None:
        data = data[data['media'] == media]
        max_install = data['install'].max()
    else:
        max_install = data['install'].max()

    if install_date_time is not None:
        data = data[data['install_date_time'] == install_date_time]

    bin_size = max_install // num_bins

    print(media, max_install)
    print(data)

    for i in range(num_bins):
        start = i * bin_size
        end = (i + 1) * bin_size
        data_bin = data[(data['install'] >= start) & (data['install'] < end)]

        # 데이터가 없을 경우 스킵
        if data_bin.empty:
            continue

        X = data_bin[['install']]  # 더미 변수 추가
        X = sm.add_constant(X)
        Y = data_bin['new_purchase']

        model = sm.OLS(Y, X).fit()
        summary = model.summary()

        # Adjusted R-squared 값을 추출하여 출력
        adjusted_r_squared = float(summary.tables[0].data[1][3])
        print(f"Regression Summary for install_{start}_{end}:")
        print(f"Adjusted R-squared: {adjusted_r_squared}")
        # print(summary)
        print("---------------next---------------")

        plt.scatter(data_bin['install'], data_bin['new_purchase'], alpha=0.5,
                    label=f'Actual data install_{start}_{end}')
        plt.plot(data_bin['install'], model.predict(), label=f'Regression Model install_{start}_{end}')

    plt.xlabel('install')
    plt.ylabel('new_purchase')
    plt.title('Regression Analysis')
    plt.legend(loc='best')  # 범례 위치를 최적으로 조정
    plt.show()

# 데이터 파일 경로 설정
result_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1720'
file_name = '/prep_data.csv'
data = load_data(result_dir + file_name)

# 선택한 media와 install_date_time에 따른 회귀 분석 실행
selected_media = 'Kakao'
input_num = 1
selected_install_date_time = None
perform_regression_analysis(data, num_bins=input_num, media=selected_media, install_date_time=selected_install_date_time)
