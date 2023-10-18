import setting.directory as dr
import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
import matplotlib.pyplot as plt
from scipy.signal import find_peaks
import warnings
warnings.filterwarnings("ignore", category=UserWarning)


def func(x, a, b, c):
    # fitting에 사용되는 모델 함수(이차 함수)
    return a * x**2 + b * x + c


def get_peaks(x, y, title):
    params, covariance = curve_fit(func, x, y, p0=None, sigma=None, absolute_sigma=False)
    # x: 독립변수
    # y: 종속변수
    # p0: 파라미터의 추측 초기값 지정 (초기값 = None, scalar or N-length sequence,선택가능)
    # sigma: least-square problem 에서의 가중치(초기값 = None, M-length sequence)
    # absolute_sigma:
    #    False : sigma는 상대가중치를 의미, 가중치의 크기가 중요하지 않고, 상대적인 비율이 중요, 반환되는 공분산 행렬인 pcov는 추측된 오차에 근거한다.
    #    True   : sigma가 입력데이터의 표준편차 오차를 의미, 반환되는 pcov는 이 값에 근거
    # params: 오차 제곱의 합이 최소가되는 파라미터의 최적의 값
    # covariance: popt의 추측 공분산, 표준편차 오차를 계산하려면 perr = np.sqrt(np.diag(pcov))를 이용
    y_fit = func(x, *params)
    plt.rc("font", family='NanumGothic')
    plt.title(title)
    plt.plot(x, y, 'bo', label='Data')
    plt.plot(x, y_fit, 'r-', label='Fitted Curve')
    plt.legend()
    plt.show()
    peaks, _ = find_peaks(y_fit)
    if len(peaks) == 0:
        result = max(x)
        peaks = len(y) - 1
        plt.annotate(f'There is no peak points.',  # 레이블 텍스트
                     xy=(result, y_fit[peaks]),  # x, y 위치
                     xytext=(0, max(y)) # 레이블 텍스트 위치
                     )
    else:
        result = x[peaks]
        plt.annotate(f'Peak_point (install:{result}, fitted_purchase{y_fit[peaks]})',  # 레이블 텍스트
                    xy=(result, y[peaks]),  # x, y 위치
                    xytext=(0, max(y))  # 레이블 텍스트 위치
                    )
    return y_fit, result


df = pd.read_csv(dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/29CM/DCT1747/prep_data.csv', encoding='utf-8')
df = df.sort_values('install')
x = np.array(df.install)
y = np.array(df.first_purchase)
y_fit, peaks = get_peaks(x, y, 'total')

install_df = df.loc[df['label'] == '인스톨'].reset_index(drop=True)
install_x = np.array(install_df.install)
install_y = np.array(install_df.first_purchase)
install_y_fit, install_peaks = get_peaks(install_x, install_y, 'install_campaign')

first_df = df.loc[df['label'] == '첫구매'].reset_index(drop=True)
first_x = np.array(first_df.install)
first_y = np.array(first_df.first_purchase)
first_y_fit, first_peaks = get_peaks(first_x, first_y, 'first_campaign')

