import logging, sys, math, ast, time, calendar, gspread, datetime
import matplotlib.pyplot as plt
import datetime as dt
import numpy as np
import pandas as pd


plt.gcf().subplots_adjust(bottom=0.8)
np.set_printoptions(threshold=sys.maxsize)
np.set_printoptions(linewidth=1000)
np.set_printoptions(precision=1)
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)
dtype = np.float64

### Checking the Accuracy of HiddenMarkovModel
ex_conv_seq = {
    dt.date(year=2022, month=1, day=1): 1,
    dt.date(year=2022, month=1, day=2): 0,
    dt.date(year=2022, month=1, day=3): 0,
    dt.date(year=2022, month=1, day=4): 1,
    dt.date(year=2022, month=1, day=5): 1,
    dt.date(year=2022, month=1, day=6): 1,
    dt.date(year=2022, month=1, day=7): 0,
    dt.date(year=2022, month=1, day=8): 1,
    dt.date(year=2022, month=1, day=9): 0,
    dt.date(year=2022, month=1, day=10): 1,
    dt.date(year=2022, month=1, day=11): 1,
    dt.date(year=2022, month=1, day=12): 0,
    dt.date(year=2022, month=1, day=13): 0,
    dt.date(year=2022, month=1, day=14): 0,
    dt.date(year=2022, month=1, day=15): 1,
    dt.date(year=2022, month=1, day=16): 0,
    dt.date(year=2022, month=1, day=17): 1,
    dt.date(year=2022, month=1, day=18): 1,
    dt.date(year=2022, month=1, day=19): 0,
}
ex_markov_mat = pd.DataFrame(
    {
        0: {0: 0.1, 1: 0.4, 2: 0.2, 3: 0.05, 4: 0.05, 5: 0.1, 6: 0.1},
        1: {0: 0.2, 1: 0.3, 2: 0.2, 3: 0.1, 4: 0.05, 5: 0.05, 6: 0.1},
        2: {0: 0.1, 1: 0.1, 2: 0.1, 3: 0.3, 4: 0.2, 5: 0.05, 6: 0.15},
        3: {0: 0.05, 1: 0.05, 2: 0.1, 3: 0.2, 4: 0.3, 5: 0.2, 6: 0.1},
        4: {0: 0.05, 1: 0.1, 2: 0.1, 3: 0.1, 4: 0.1, 5: 0.2, 6: 0.35},
        5: {0: 0.1, 1: 0.15, 2: 0.25, 3: 0.2, 4: 0.2, 5: 0.05, 6: 0.05},
        6: {0: 0.2, 1: 0.1, 2: 0.1, 3: 0.2, 4: 0.2, 5: 0.1, 6: 0.1},
    }
).transpose()
ex_start_prob = [0.15, 0.1, 0.15, 0.15, 0.15, 0.15, 0.15]
ex_emission_mat = pd.DataFrame(
    {
        0: {0: 0.8, 1: 0.2},
        1: {0: 0.7, 1: 0.3},
        2: {0: 0.6, 1: 0.4},
        3: {0: 0.7, 1: 0.3},
        4: {0: 0.6, 1: 0.4},
        5: {0: 0.9, 1: 0.1},
        6: {0: 0.7, 1: 0.3},
    }
).transpose()
### Checking the Accuracy of HiddenMarkovModel



#
#
# class HiddenMarkovtModel:
#     def __init__(self):
#         self.file = None
#         self.conv_seq = None
#         self.markov_mat = None
#         self.start_prob = None
#         self.emission_mat = None
#         self.media_list = None
#         self.media_conv = None
#         self.start_media = None
#         self.state = None
#         self.time = None
#         self.obs = None
#         self.example = False
#         self.test_mode = False
#
#     def forward_algorithm(self):
#         forward = np.zeros((self.state, self.time), dtype=dtype)
#         forward[:, 0] = self.start_prob * self.emission_mat[:, self.conv_seq[0]]
#         forward_p = np.zeros((self.state, self.time), dtype=dtype)
#         forward_p[:, 0] = self.start_prob * self.emission_mat[:, self.conv_seq[0]]
#         for time in range(1, self.time):
#             ### for문으로 이해하기 쉬운 코드 ###
#             if self.example:
#                 for state in range(self.state):
#                     state_sum = 0.0
#                     for before_state in range(self.state):
#                         state_sum += (
#                             self.markov_mat[before_state, state]
#                             * self.emission_mat[state, self.conv_seq[time]]
#                             * forward[before_state, time - 1]
#                         )
#                     forward_p[state, time] = state_sum
#             ### numpy로 간소화한 코드 ###
#             forward[:, time] = np.dot(
#                 (self.markov_mat * self.emission_mat[:, self.conv_seq[time]]).transpose(),
#                 forward[:, time - 1],
#             )
#
#         if self.example:
#             print(np.linalg.norm(forward) - np.linalg.norm(forward_p))
#
#         return forward
#
#     def backward_algorithm(self):
#         backward = np.zeros(shape=(self.state, self.time), dtype=dtype)
#         backward[:, self.time - 1] = 1
#         backward_p = np.zeros(shape=(self.state, self.time), dtype=dtype)
#         backward_p[:, self.time - 1] = 1
#         for time in range(self.time - 2, -1, -1):
#             ### for문으로 이해하기 쉬운 코드 ###
#             if self.example:
#                 for state in range(self.state):
#                     state_sum = 0.0
#                     for after_state in range(self.state):
#                         state_sum += (
#                             self.markov_mat[state, after_state]
#                             * self.emission_mat[after_state, self.conv_seq[time + 1]]
#                             * backward[after_state, time + 1]
#                         )
#                     backward_p[state, time] = state_sum
#             ### numpy로 간소화한 코드 ###
#             backward[:, time] = np.dot(
#                 self.markov_mat,
#                 (self.emission_mat[:, self.conv_seq[time + 1]] * backward[:, time + 1]),
#             )
#
#         if self.example:
#             print(np.linalg.norm(backward) - np.linalg.norm(backward_p))
#
#         return backward
#
#     def posterior_decode(self, forward, backward):
#         state_posterior = np.zeros(shape=(self.state, self.time), dtype=dtype)
#         state_posterior_p = np.zeros(shape=(self.state, self.time), dtype=dtype)
#         trans_posterior = np.zeros(shape=(self.state, self.state, self.time), dtype=dtype)
#         trans_posterior_p = np.zeros(shape=(self.state, self.state, self.time), dtype=dtype)
#         for time in range(self.time):
#             ### for문으로 이해하기 쉬운 코드 ###
#             if self.example:
#                 state_denominator = 0.0
#                 for state in range(self.state):
#                     state_prob = forward[state, time] * backward[state, time]
#                     state_posterior_p[state, time] = state_prob
#                     state_denominator += state_prob
#                 for state in range(self.state):
#                     if state_denominator > 0:
#                         state_posterior_p[state, time] /= state_denominator
#             ### numpy로 간소화한 코드 ###
#             state_posterior[:, time] = forward[:, time] * backward[:, time]
#             if state_posterior[:, time].sum() > 0:
#                 state_posterior[:, time] /= state_posterior[:, time].sum()
#
#             if time < self.time - 1:
#                 ### for문으로 이해하기 쉬운 코드 ###
#                 if self.example:
#                     trans_denominator = 0.0
#                     for state in range(self.state):
#                         for after_state in range(self.state):
#                             trans_prob = (
#                                 self.markov_mat[state, after_state]
#                                 * self.emission_mat[after_state, self.conv_seq[time + 1]]
#                                 * forward[state, time]
#                                 * backward[state, time + 1]
#                             )
#                             trans_posterior_p[state, after_state, time] = trans_prob
#                             trans_denominator += trans_prob
#                     for state in range(self.state):
#                         for after_state in range(self.state):
#                             if trans_denominator > 0:
#                                 trans_posterior_p[state, after_state, time] /= trans_denominator
#                 ### numpy로 간소화한 코드 ###
#                 trans_posterior[:, :, time] = (
#                     (self.markov_mat * self.emission_mat[:, self.conv_seq[time + 1]]).transpose()
#                     * forward[:, time]
#                     * backward[:, time + 1]
#                 ).transpose()
#                 if trans_posterior[:, :, time].sum() > 0:
#                     trans_posterior[:, :, time] /= trans_posterior[:, :, time].sum()
#
#         if self.test_mode:
#             print(
#                 f"posterior - sum of state: {round(state_posterior.sum(), 0)}, sum of trans: {round(trans_posterior.sum(), 0)} for 1T~{self.time}T"
#             )
#
#         if self.example:
#             print(np.linalg.norm(state_posterior) - np.linalg.norm(state_posterior_p))
#             print(np.linalg.norm(trans_posterior) - np.linalg.norm(trans_posterior_p))
#
#         return state_posterior, trans_posterior
#
#     def expectation_maximization_algorithm(self):
#         self.state, self.time, self.obs = (
#             self.markov_mat.shape[0],
#             len(self.conv_seq),
#             self.emission_mat.shape[1],
#         )
#         self.conv_seq, self.markov_mat, self.start_prob, self.emission_mat = (
#             np.asarray([value for key, value in sorted(self.conv_seq.items())]),
#             np.asarray(self.markov_mat),
#             np.asarray(self.start_prob),
#             np.asarray(self.emission_mat),
#         )
#
#         cnt, ft = 0, None
#         while True:
#             before = dt.datetime.now()
#             if cnt == 0:
#                 ft = before
#             forward = self.forward_algorithm()
#             backward = self.backward_algorithm()
#             expectation_state, expectation_trans = self.posterior_decode(
#                 forward=forward,
#                 backward=backward,
#             )
#
#             maxima_trans = np.zeros((self.state, self.state), dtype=dtype)
#             maxima_emission = np.zeros((self.state, self.obs), dtype=dtype)
#             maxima_trans_p = np.zeros((self.state, self.state), dtype=dtype)
#             maxima_emission_p = np.zeros((self.state, self.obs), dtype=dtype)
#             for state in range(self.state):
#                 ### for문으로 이해하기 쉬운 코드 ###
#                 if self.example:
#                     state_time_sum = 0.0
#                     for time in range(self.time - 1):
#                         state_time_sum += expectation_state[state, time]
#                     for after_state in range(self.state):
#                         if state_time_sum > 0:
#                             trans_time_sum = 0.0
#                             for time in range(self.time - 1):
#                                 trans_time_sum += expectation_trans[state, after_state, time]
#                             maxima_trans_p[state, after_state] = trans_time_sum / state_time_sum
#
#                     state_time_sum += expectation_state[state, self.time - 1]
#                     emission_seq_sum = np.asarray([0.0 for i in range(self.obs)])
#                     for time in range(self.time):
#                         emission_seq_sum[self.conv_seq[time]] += expectation_state[state, time]
#                     if state_time_sum > 0:
#                         for obs in range(self.obs):
#                             maxima_emission_p[state, obs] = emission_seq_sum[obs] / state_time_sum
#
#                 ### numpy로 간소화한 코드 ###
#                 if expectation_state[state, : self.time - 1].sum() > 0:
#                     maxima_trans[state, :] = (
#                         expectation_trans[state, :, : self.time - 1].sum(axis=1)
#                         / expectation_state[state, : self.time - 1].sum()
#                     )
#
#                 conv_seq = np.asarray(self.conv_seq)
#                 if expectation_state[state, :].sum() > 0:
#                     maxima_emission[state, :] = (
#                         np.asarray(
#                             [expectation_state[state, np.where(conv_seq == obs)].sum() for obs in range(self.obs)]
#                         )
#                         / expectation_state[state, :].sum()
#                     )
#
#             if self.example:
#                 print(np.linalg.norm(maxima_trans) - np.linalg.norm(maxima_trans_p))
#                 print(np.linalg.norm(maxima_emission) - np.linalg.norm(maxima_emission_p))
#
#             after, cnt = dt.datetime.now(), cnt + 1
#             if self.test_mode:
#                 print(
#                     f"{cnt} step during {round((after - before).microseconds / 1000000, 2)} sec. whole {round((after - ft).seconds / 60, 2)} min"
#                 )
#                 print(
#                     f"{maxima_trans.shape} markov mat sum: {round(maxima_trans.sum(), 0)}, {maxima_emission.shape} emission mat sum: {round(maxima_emission.sum(), 0)} on {self.state} states"
#                 )
#                 markov_diff = np.format_float_scientific(np.linalg.norm(self.markov_mat - maxima_trans), precision=1)
#                 emission_diff = np.format_float_scientific(
#                     np.linalg.norm(self.emission_mat - maxima_emission), precision=1
#                 )
#                 print(f"markov mat diff: {markov_diff}, emission mat diff: {emission_diff}")
#             if (
#                 np.linalg.norm(self.markov_mat - maxima_trans) <= 0.01
#                 and np.linalg.norm(self.emission_mat - maxima_emission) < 0.01
#             ):
#                 self.markov_mat, self.emission_mat = maxima_trans, maxima_emission
#                 print(f"elapsed time was {round((after - ft).seconds / 60, 2)} min")
#                 break
#             else:
#                 self.markov_mat, self.emission_mat = maxima_trans, maxima_emission
#
#     def recalibrate(self):
#         ### 최적화 이후 재규격화
#         for state in range(self.state):
#             if self.markov_mat[state, :].sum() > 0:
#                 self.markov_mat[state, :] = self.markov_mat[state, :] / self.markov_mat[state, :].sum()
#
#         markov_mat, denominator = dict(), 0
#         ### conv 상태 전이 확률 추가를 위한 분모 스케일링
#         for media in self.media_list:
#             if media in self.media_conv.keys():
#                 conv_scale = 10.0 ** len(str(self.media_conv[media]))
#                 if denominator < conv_scale:
#                     denominator = conv_scale
#
#         ### conv 상태로 전이될 확률 추가 및 기존 전이확률 업데이트
#         for i in range(len(self.media_list)):
#             if not self.media_list[i] in markov_mat.keys():
#                 markov_mat[self.media_list[i]] = dict()
#             if self.media_list[i] in self.media_conv.keys():
#                 updated_denominator = denominator + self.media_conv[self.media_list[i]]
#                 for j in range(len(self.media_list)):
#                     markov_mat[self.media_list[i]][self.media_list[j]] = (
#                         self.markov_mat[i, j] * denominator / updated_denominator
#                     )
#                 markov_mat[self.media_list[i]]["conv"] = self.media_conv[self.media_list[i]] / updated_denominator
#             else:
#                 for j in range(len(self.media_list)):
#                     markov_mat[self.media_list[i]][self.media_list[j]] = self.markov_mat[i, j]
#                 markov_mat[self.media_list[i]]["conv"] = 0
#
#         ### 흡수 상태 확률 추가
#         absorb_state = ["start", "conv", "null"]
#         for state in absorb_state:
#             markov_mat[state] = dict()
#             if state == "start":
#                 for media in self.media_list:
#                     markov_mat[state][media] = self.start_media[media]
#                 markov_mat[state][state] = 0
#             else:
#                 markov_mat[state][state] = 1
#
#         return markov_mat
#
#     def preprocess(self, af_dict, media_list, media_event):
#         self.media_list = media_list
#         emission_mat = dict()
#         for media in self.media_list:
#             if not media in emission_mat.keys():
#                 emission_mat[media] = dict()
#             prob = (
#                 media_event[media]["first_purchase"] / media_event[media]["af_content_view"]
#                 if "first_purchase" in media_event[media].keys()
#                 else 0
#             )
#             emission_mat[media][0] = 1.0 - prob
#             emission_mat[media][1] = prob
#
#         first_media, self.start_media = dict(), dict()
#         for val in af_dict.values():
#             if not val[0][1] in first_media.keys():
#                 first_media[val[0][1]] = 0
#             first_media[val[0][1]] += 1
#         denominator = sum(first_media.values())
#         for media in self.media_list:
#             self.start_media[media] = first_media[media] / denominator if media in first_media.keys() else 0
#
#         media_trans, markov_mat = dict(), dict()
#         for val in af_dict.values():
#             for state in range(len(val) - 1):
#                 if not val[state][1] in media_trans.keys():
#                     media_trans[val[state][1]] = dict()
#                 if not val[state + 1][1] in media_trans[val[state][1]].keys():
#                     media_trans[val[state][1]][val[state + 1][1]] = 0
#                 media_trans[val[state][1]][val[state + 1][1]] += 1
#         for fmedia in self.media_list:
#             denominator = sum(media_trans[fmedia].values()) if fmedia in media_trans.keys() else 0
#             if not fmedia in markov_mat.keys():
#                 markov_mat[fmedia] = dict()
#             for tmedia in self.media_list:
#                 markov_mat[fmedia][tmedia] = (
#                     media_trans[fmedia][tmedia] / denominator
#                     if denominator > 0 and tmedia in media_trans[fmedia].keys()
#                     else 0
#                 )
#
#         af_seq = sorted([ent for val in af_dict.values() for ent in val])
#         observation_value = {ent[0]: ent[2] for ent in af_seq}
#
#         self.media_conv = dict()
#         for ent in af_seq:
#             if ent[2] == 1:
#                 if not ent[1] in self.media_conv.keys():
#                     self.media_conv[ent[1]] = 0
#                 self.media_conv[ent[1]] += 1
#
#         self.conv_seq = observation_value
#         self.markov_mat = pd.DataFrame(markov_mat).transpose()  # EM algorithm -> MC Attribution
#         self.start_prob = list(self.start_media.values())  # MC Attribution Matrix Preprocessing
#         self.emission_mat = pd.DataFrame(emission_mat).transpose()  # EM algorithm -> Complete
#
#     def optimize(self, total_conv):
#         if self.example:
#             self.conv_seq = ex_conv_seq
#             self.markov_mat = ex_markov_mat
#             self.start_prob = ex_start_prob
#             self.emission_mat = ex_emission_mat
#
#         print(f"num of state: {len(self.markov_mat)}, num of seq: {len(self.conv_seq)}")
#         conv_val = [val for val in self.conv_seq.values()]
#         for i in range(math.ceil(len(self.media_list) / 7)):
#             print(f"media: {self.media_list[i*7:(i+1)*7]}")
#         for i in range(math.ceil(len(self.conv_seq) / 40)):
#             print(f"{(i+1)*40} seq: {conv_val[i*40:(i+1)*40]}")
#         self.expectation_maximization_algorithm()
#         markov_chain = MarkovChainAttribution(df=None, total_conv=None, null_exists=None)
#         markov_mat = self.recalibrate()
#         ref_val = markov_chain.removable_effect(markov_mat=markov_mat, all_channels=self.media_list)
#         results = markov_chain.attribution_distribution(ref_val=ref_val)
#         attribution = {
#             media: pct * total_conv
#             for media, pct in sorted(results.items(), key=lambda item: item[1], reverse=True)  # if pct >= 0.001
#         }
#
#         return attribution
#
#
# def AttributionModel_Comparison(df):
#     start = time.time()  # 시작 시간 저장
#
#     for_other_df = df.copy()
#
#     basic_model = BasicAttribution(df=for_other_df, col_name="total_conversions")
#     shap_model = ShapAttribution(df=for_other_df, col_name="total_conversions")
#     linear_df = basic_model.linear_attribution()
#     last_click_df = basic_model.last_click_attribution()
#     shap_df = shap_model.attribution()
#     Total_attributionModel = pd.concat([linear_df, last_click_df, shap_df], axis=1)
#     Total_attributionModel.columns = [
#         "Linear Model",
#         "Lastclick Model",
#         "ShapleyValue Model",
#     ]
#
#     end = time.time() - start
#     print("time: ", end, " 초 걸렸습니다.")
#
#     return Total_attributionModel
#
#
# class Analysis(GetAfData):
#     def __init__(self, owner_id, redshift, f_path, csv, basic, shap, mc_on_hmm):
#         GetAfData.__init__(self)
#
#         self.owner_id = owner_id
#         self.basic = basic
#         self.shap = shap
#         self.mc_on_hmm = mc_on_hmm
#         self.redshift = redshift
#         self.af_data = None
#         self.media = set()
#         self.dates = list()
#         self.basic_attr_list = dict()
#         self.mc_attr_list = dict()
#         self.hmm_attr_list = dict()
#         self.total_conv_list = dict()
#         self.max_attr = 0
#         self.count_path = dict()
#         self.avg_attr = dict()
#
#         ### Load File
#         if csv:
#             self.load_csv(f_path=f_path, bm=3, em=5)
#
#     def for_redshift(self, first_date, days):
#         for day in range(days):
#             start_time = (first_date + dt.timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S")
#             end_time = (first_date + dt.timedelta(days=day + 1)).strftime("%Y-%m-%d %H:%M:%S")
#             print(start_time)
#             ### Collect Data
#             df, af_dict, media_list, media_event, total_conv = self.collect(
#                 owner_id=self.owner_id,
#                 start_time=start_time,
#                 end_time=end_time,
#                 redshift=self.redshift,
#                 media=True,
#                 account=False,
#             )
#             self.dates.append(start_time)
#
#             if self.basic:
#                 data = pd.DataFrame(df).transpose()
#                 basic_model = BasicAttribution(df=data, col_name="conv")
#                 linear_attr = basic_model.linear_attribution()
#                 last_attr = basic_model.last_click_attribution()
#
#             ### SHAP Part
#             if self.shap:
#                 shapley = ShapAttribution(df=df, col_name="conv")
#                 ret = shapley.attribution()
#
#             if self.mc_on_hmm:
#                 ### Markov Chain and Hidden Markov Part
#                 mc_attr = MarkovChainAttribution(df=df, total_conv=total_conv, null_exists=False)
#                 ret = mc_attr.run(media_list=media_list)
#                 for key, val in ret.items():
#                     self.media.add(key)
#                     if not key in self.mc_attr_list.keys():
#                         self.mc_attr_list[key] = dict()
#                     self.mc_attr_list[key][start_time] = val
#                     if self.max_attr < val:
#                         self.max_attr = val
#
#                 ### Hidden Markov Model Part
#                 hmm = HiddenMarkovtModel()
#                 hmm.preprocess(af_dict=af_dict, media_list=media_list, media_event=media_event)
#                 ret = hmm.optimize(total_conv=total_conv)
#                 for key, val in ret.items():
#                     self.media.add(key)
#                     if not key in self.hmm_attr_list.keys():
#                         self.hmm_attr_list[key] = dict()
#                     self.hmm_attr_list[key][start_time] = val
#                     if self.max_attr < val:
#                         self.max_attr = val
#
#         for key in list(self.media):
#             for date in self.dates:
#                 if not key in self.mc_attr_list.keys():
#                     self.mc_attr_list[key] = dict()
#                 if not date in self.mc_attr_list[key].keys():
#                     self.mc_attr_list[key][date] = 0
#                 if not key in self.hmm_attr_list.keys():
#                     self.hmm_attr_list[key] = dict()
#                 if not date in self.hmm_attr_list[key].keys():
#                     self.hmm_attr_list[key][date] = 0
#
#         for key in list(self.media):
#             self.avg_attr[key] = np.average(
#                 [val for val in self.mc_attr_list[key].values()] + [val for val in self.hmm_attr_list[key].values()]
#             )
#
#     def for_csv(self, first_date, last_date, period):
#         for day in range(0, (last_date + dt.timedelta(seconds=1) - first_date).days, period):
#             alpha_2, date_check = dt.datetime.now(), False
#             start_time = pd.to_datetime((first_date + dt.timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S"))
#             to_time = first_date + dt.timedelta(days=day + period, seconds=-1)
#             end_time = (
#                 pd.to_datetime(to_time.strftime("%Y-%m-%d %H:%M:%S"))
#                 if (last_date.date() - to_time.date()).days >= period
#                 else pd.to_datetime(last_date.strftime("%Y-%m-%d %H:%M:%S"))
#             )
#             if end_time == pd.to_datetime(last_date.strftime("%Y-%m-%d %H:%M:%S")):
#                 date_check = True
#
#             print(start_time, end_time)
#             df, af_dict, media_list, media_event, total_conv = self.collect(
#                 owner_id=self.owner_id,
#                 start_time=start_time,
#                 end_time=end_time,
#                 redshift=self.redshift,
#                 media=False,
#                 account=True,
#             )
#             self.dates.append(start_time)
#             ### media Attributed conversion by path
#             for id, val in df.items():
#                 path = ",".join(val["path"])
#                 if not path in self.count_path.keys():
#                     self.count_path[path] = val["conv"]
#                 else:
#                     self.count_path[path] += val["conv"]
#
#             print("Total conversion:", total_conv)
#             if total_conv > 0:
#                 for key in media_list:
#                     if key in media_event.keys():
#                         if not key in self.total_conv_list.keys():
#                             self.total_conv_list[key] = dict()
#                         if not start_time in self.total_conv_list[key].keys():
#                             self.total_conv_list[key][start_time] = 0
#                         self.total_conv_list[key][start_time] += media_event[key]["first_purchase"]
#                 alpha_3 = dt.datetime.now()
#                 print(f"Data collect and preprocess {(alpha_3 - alpha_2).seconds / 60} min")
#
#                 if self.basic:
#                     data = pd.DataFrame(df).transpose()
#                     basic_model = BasicAttribution(df=data, col_name="conv")
#                     linear_attr = basic_model.linear_attribution()
#                     print(linear_attr)
#                     last_attr = basic_model.last_click_attribution()
#                     print(last_attr)
#
#                 if self.mc_on_hmm:
#                     ### Markov Chain Part
#                     mc_attr = MarkovChainAttribution(df=df, total_conv=total_conv, null_exists=True)
#                     ret = mc_attr.run(media_list=media_list)
#                     alpha_4 = dt.datetime.now()
#                     print(f"MC running {(alpha_4 - alpha_3).seconds / 60} min")
#                     for key, val in ret.items():
#                         self.media.add(key)
#                         if not key in self.mc_attr_list.keys():
#                             self.mc_attr_list[key] = dict()
#                         self.mc_attr_list[key][start_time] = val
#                         if self.max_attr < val:
#                             self.max_attr = val
#
#                     ### Hidden Markov Model Part
#                     hmm = HiddenMarkovtModel()
#                     hmm.preprocess(af_dict=af_dict, media_list=media_list, media_event=media_event)
#                     ret = hmm.optimize(total_conv=total_conv)
#                     alpha_5 = dt.datetime.now()
#                     print(f"HMM running {(alpha_5 - alpha_4).seconds / 60} min")
#
#                     for key, val in ret.items():
#                         self.media.add(key)
#                         if not key in self.hmm_attr_list.keys():
#                             self.hmm_attr_list[key] = dict()
#                         self.hmm_attr_list[key][start_time] = val
#                         if self.max_attr < val:
#                             self.max_attr = val
#
#             if date_check:
#                 break
#
#         for key in list(self.media):
#             for date in self.dates:
#                 if not key in self.mc_attr_list.keys():
#                     self.mc_attr_list[key] = dict()
#                 if not date in self.mc_attr_list[key].keys():
#                     self.mc_attr_list[key][date] = 0
#                 if not key in self.hmm_attr_list.keys():
#                     self.hmm_attr_list[key] = dict()
#                 if not date in self.hmm_attr_list[key].keys():
#                     self.hmm_attr_list[key][date] = 0
#
#
# class OutputExport:
#     def __init__(self, media, dates, mc_attr_list, hmm_attr_list, max_attr):
#         self.mc_attr_list = mc_attr_list
#         self.hmm_attr_list = hmm_attr_list
#         self.media = media
#         self.dates = dates
#         self.max_attr = max_attr
#
#     def result_csv(self, total_conv_list):
#         output = open(f"{f_path}/results.csv", "w")
#         output.write(f"account,start_date,conversions,mc_attribution,hmm_attribution\n")
#         for key in list(self.media):
#             for date in self.dates:
#                 conversions = (
#                     total_conv_list[key][date]
#                     if key in total_conv_list.keys() and date in total_conv_list[key].keys()
#                     else 0
#                 )
#                 mc_attribution = (
#                     self.mc_attr_list[key][date]
#                     if key in self.mc_attr_list.keys() and date in self.mc_attr_list[key].keys()
#                     else 0
#                 )
#                 hmm_attribution = (
#                     self.hmm_attr_list[key][date]
#                     if key in self.hmm_attr_list.keys() and date in self.hmm_attr_list[key].keys()
#                     else 0
#                 )
#                 output.write(f"{key},{date},{conversions},{mc_attribution},{hmm_attribution}\n")
#
#         output.close()
#
#     def check_result(self, af_data, count_path):
#         media_attr_conv = dict()
#         for path, conv in count_path.items():
#             for media_name in list(self.media):
#                 if media_name in path:
#                     if not media_name in media_attr_conv.keys():
#                         media_attr_conv[media_name] = conv
#                     else:
#                         media_attr_conv[media_name] += conv
#
#         event_name_list = list(set(np.asarray(af_data["event_name"].drop_duplicates())))
#         media_event = {
#             key: {
#                 event: af_data[(af_data["account"] == key) & (af_data["event_name"] == event)].shape[0]
#                 for event in event_name_list
#             }
#             for key in list(self.media)
#         }
#
#         print("MC simplifying", media_attr_conv)
#         print(
#             "HMM simplifying",
#             {
#                 media_name: media_event[media_name]["first_purchase"] / media_event[media_name]["af_content_view"]
#                 for media_name in list(self.media)
#             },
#         )
#
#     def spreadsheet(self, avg_attr):
#         s3path = s3.S3FilePath()
#         s3path.path = "argo-data-container/v3/resource/lever-spreadsheet.json"
#         credential_file = s3.download_file(s3path)
#         gc = gspread.service_account(filename=credential_file)
#
#         sh = gc.open("Attribution Analysis Report")
#         wks = sh.worksheet(title="Weekly Report")
#         cell_list, media = (
#             list(),
#             [key for key, val in sorted(avg_attr.items(), key=lambda item: item[1], reverse=True)][:20],
#         )
#         requests = [
#             {
#                 "updateSheetProperties": {
#                     "properties": {
#                         "gridProperties": {
#                             "rowCount": len(self.dates) * len(media) + 1,
#                             "columnCount": 4,
#                         },
#                         "sheetId": wks.id,
#                     },
#                     "fields": "gridProperties",
#                 }
#             }
#         ]
#         sh.values_clear(f"Weekly Report!A1:D{len(self.dates) * len(media) + 1}")
#         cell_range = wks.range(f"A1:D{len(self.dates) * len(media) + 1}")
#         sh.batch_update({"requests": requests})
#         header = ["date", "media", "model", "attr"]
#         date_cnt, medium_cnt, model_switch = 0, 0, 0
#         for row in range(len(self.dates) * len(media) * 2 + 1):
#             row_cells, seq = cell_range[row * 4 : row * 4 + 4], 0
#             for cell in row_cells:
#                 if row == 0:
#                     cell.value = header[seq]
#                 else:
#                     if seq == 0:
#                         cell.value = self.dates[date_cnt]
#                     if seq == 1:
#                         cell.value = media[medium_cnt]
#                     if seq == 2:
#                         cell.value = "mc" if model_switch == 0 else "hmm"
#                     if seq == 3:
#                         cell.value = (
#                             self.mc_attr_list[media[medium_cnt]][self.dates[date_cnt]]
#                             if model_switch == 0
#                             else self.hmm_attr_list[media[medium_cnt]][self.dates[date_cnt]]
#                         )
#                 cell_list.append(cell)
#                 seq += 1
#             if row > 0:
#                 model_switch += 1
#                 if model_switch == 2:
#                     date_cnt += 1
#                     model_switch = 0
#                     if date_cnt == 7:
#                         medium_cnt += 1
#                         date_cnt = 0
#
#         wks.update_cells(cell_list=cell_list)
#
#     def plotting(self):
#         bar_half = 0.025
#         bar_graph, curve_graph = False, True
#         plt.rcParams["font.family"] = "AppleGothic"
#         plt.figure(figsize=(25, 7))
#
#         if curve_graph:
#             for j in range(len(self.mc_attr_list.keys())):
#                 x_pos = [7 * j + 1 + 2 * bar_half * i[0] for i in enumerate(self.dates)]
#                 media_attr = [sorted(self.mc_attr_list.items())[j][1][i[1]] for i in enumerate(self.dates)]
#                 plt.plot(x_pos, media_attr)
#
#         if bar_graph:
#             for j in enumerate(self.dates):
#                 x_pos = [7 * i + 1 + 2 * bar_half * j[0] for i in range(len(self.mc_attr_list.keys()))]
#                 media_attr = [ent[1][j[1]] for ent in sorted(self.mc_attr_list.items())]
#                 plt.bar(x_pos, media_attr, width=2 * bar_half)
#
#         plt.xticks(
#             [7 * i + 1 + bar_half * (len(self.dates) - 1) for i in range(len(self.mc_attr_list.keys()))],
#             [key for key in sorted(self.mc_attr_list.keys())],
#             fontsize=15,
#             rotation=0,
#         )
#         max_num, max_dec = (
#             int(str(int(self.max_attr))[0]) + 1,
#             len(str(int(self.max_attr))) - 1,
#         )
#         plt.ylim(0, max_num * 10**max_dec - 50)
#         plt.savefig(f"/Users/zane.chang/Desktop/attr_per_day_mc.png", bbox_inches="tight")
#
#         plt.close()
#         plt.figure(figsize=(25, 7))
#
#         if curve_graph:
#             for j in range(len(self.hmm_attr_list.keys())):
#                 x_pos = [7 * j + 1 + 2 * bar_half * i[0] for i in enumerate(self.dates)]
#                 media_attr = [sorted(self.hmm_attr_list.items())[j][1][i[1]] for i in enumerate(self.dates)]
#                 plt.plot(x_pos, media_attr)
#
#         if bar_graph:
#             for j in enumerate(self.dates):
#                 x_pos = [7 * i + 1 + 2 * bar_half * j[0] for i in range(len(self.hmm_attr_list.keys()))]
#                 media_attr = [ent[1][j[1]] for ent in sorted(self.hmm_attr_list.items())]
#                 plt.bar(x_pos, media_attr, width=2 * bar_half)
#
#         plt.xticks(
#             [7 * i + 1 + bar_half * (len(self.dates) - 1) for i in range(len(self.hmm_attr_list.keys()))],
#             [key for key in sorted(self.hmm_attr_list.keys())],
#             fontsize=15,
#             rotation=0,
#         )
#         max_num, max_dec = (
#             int(str(int(self.max_attr))[0]) + 1,
#             len(str(int(self.max_attr))) - 1,
#         )
#         plt.ylim(0, max_num * 10**max_dec - 50)
#         plt.savefig(f"/Users/zane.chang/Desktop/attr_per_day_hmm.png", bbox_inches="tight")
#         plt.close()
#
#
# class AttribModelRunner(AlbamonWorker):
#     def __init__(self, header, body):
#         AlbamonWorker.__init__(self, header=header, body=body)
#
#         self.owner_id = None
#         self.product_id = None
#         self.schedule_dt: datetime = None
#         self.f_path = None
#         self.mc_with_hmm = None
#         self.mc_only = None
#         self.basic = None
#         self.shap = None
#         self.mc_on_hmm = None
#         self.redshift = None
#         self.csv = None
#         self.getout = None
#         self.check = None
#         self.spreadsheet = None
#         self.plot = None
#         self.result = Result()
#
#     def analyzer(self):
#         if self.mc_with_hmm:
#             alpha = dt.datetime.now()
#             analyzer = Analysis(
#                 owner_id=self.owner_id,
#                 redshift=self.redshift,
#                 f_path=self.f_path,
#                 csv=self.csv,
#                 basic=self.basic,
#                 shap=self.shap,
#                 mc_on_hmm=self.mc_on_hmm,
#             )
#
#             if self.redshift:
#                 first_date, days = dt.date(year=2022, month=3, day=1), 7
#                 analyzer.for_redshift(first_date=first_date, days=days)
#             if self.csv:
#                 first_date = dt.datetime(year=2022, month=3, day=1, hour=0, minute=0, second=0)
#                 last_date = dt.datetime(
#                     year=2022,
#                     month=5,
#                     day=calendar.monthrange(year=2022, month=5)[1],
#                     hour=23,
#                     minute=59,
#                     second=59,
#                 )
#                 analyzer.for_csv(first_date=first_date, last_date=last_date, period=1)
#
#             Output = OutputExport(
#                 media=analyzer.media,
#                 dates=analyzer.dates,
#                 mc_attr_list=analyzer.mc_attr_list,
#                 hmm_attr_list=analyzer.hmm_attr_list,
#                 max_attr=analyzer.max_attr,
#             )
#
#             if self.getout:
#                 Output.result_csv(total_conv_list=analyzer.total_conv_list)
#
#             if self.check:
#                 Output.check_result(af_data=analyzer.af_data, count_path=analyzer.count_path)
#
#             if self.spreadsheet:
#                 Output.spreadsheet(avg_attr=analyzer.avg_attr)
#
#             if self.plot:
#                 Output.plotting()
#
#             omega = dt.datetime.now()
#             print(f"whole elapsed time: {(omega - alpha).seconds / 60}m")
#
#             return None
#
#         if self.mc_only:
#             df = None
#             path_equal_or_more_than_3 = False
#             path_without_self_rotation = True
#             in_file, out_file, ret_file = None, None, None
#             if path_equal_or_more_than_3:
#                 media_event = dict()
#                 data = pd.read_csv(filepath_or_buffer=f"{f_path}/mc_data.csv")
#                 data = data[(data["event_name"] != "af_purchase")]
#                 data["event_time"] = pd.to_datetime(data["event_time"])
#                 fp_data = data[data["event_name"] == "first_purchase"][["appsflyer_id", "event_time"]]
#                 data = pd.merge(
#                     left=data,
#                     right=fp_data,
#                     on=["appsflyer_id"],
#                     suffixes=["_1", "_2"],
#                     how="left",
#                 )
#                 data = data[pd.notnull(data["event_time_2"])]
#                 data["date_diff"] = data["event_time_2"] - data["event_time_1"]
#                 data = data[(data["date_diff"].dt.days <= 6) & (data["date_diff"].dt.days >= 0)]
#                 data["path"] = data[["event_time_1", "account", "obs"]].apply(
#                     lambda x: [ent for ent in np.asarray(x)], axis=1
#                 )
#                 data = data.groupby("appsflyer_id")["path"].apply(list).to_dict()
#                 print(f"including unested path: {len(data.keys())}")
#
#                 for id, val in data.items():
#                     if not id in media_event.keys():
#                         media_event[id] = list()
#                     for ent in sorted(val):
#                         if len(media_event[id]) == 0 or media_event[id][-1][0] != ent[0] or media_event[id][-1][2] != 1:
#                             media_event[id].append(ent)
#
#                 af_dict = {id: val for id, val in media_event.items() if len(val) >= 3 and val[-1][2] == 1}
#                 print(f"length equal or more than 3 path: {len(af_dict.keys())}")
#                 df = {
#                     id: {
#                         "path": [ent[1] for ent in val[:-1]],
#                         "conv": sum([ent[2] for ent in val]),
#                     }
#                     for id, val in af_dict.items()
#                 }
#                 pd.DataFrame(df).transpose().to_csv(path_or_buf=f"/Users/zane.chang/path_data.csv", sep=",")
#
#             if path_without_self_rotation:
#                 in_file = f"full_path_data_over3_True_202207.csv"
#                 out_file = f"{in_file.replace('.csv', '_removal_eff.csv')}"
#                 ret_file = f"{in_file.replace('.csv', '_results.csv')}"
#                 data = pd.read_csv(f"{f_path}/{in_file}")
#                 data = data[(data["First Purchase"] == 1) & (data["len_of_path"] >= 3)]
#                 data = data[["id", "path", "First Purchase"]]
#                 data.columns = ["id", "path", "conv"]
#                 data["path"] = data["path"].apply(lambda x: ast.literal_eval(x)[:-1])
#                 data.set_index(data["id"], inplace=True)
#                 df = data[["path", "conv"]].transpose().to_dict()
#
#             mc_attr = MarkovChainAttribution(df=df, total_conv=None, null_exists=True)
#             ret = mc_attr.run(
#                 media_list=None,
#                 re_file=f"{f_path}/{out_file}",
#                 mc_file=f"{f_path}/{ret_file}",
#             )
#
#             return ret
#
#     def do_work(self, msg_header: dict, msg_body: dict):
#         msg_header.update(msg_body)
#         msg = JobMessage(msg_header)
#
#         self.owner_id = msg.get_job_attr(JobMessageKey.OWNER_ID)
#         self.product_id = msg.get_job_attr(JobMessageKey.PRODUCT_ID)
#         self.schedule_dt = msg.get_job_attr(JobMessageKey.SCHEDULE_TIME)
#         self.info = msg.get_job_data()
#         self.info["owner_id"] = self.owner_id
#         self.info["product_id"] = self.product_id
#         for key in self.info.keys():
#             if self.info[key] == "true":
#                 self.info[key] = True
#             elif self.info[key] == "false":
#                 self.info[key] = False
#         self.f_path = self.info["f_path"]
#         self.mc_with_hmm = self.info["mc_with_hmm"]
#         self.mc_only = self.info["mc_only"]
#         self.basic = self.info["basic"]
#         self.shap = self.info["shap"]
#         self.mc_on_hmm = self.info["mc_on_hmm"]
#         self.redshift = self.info["redshift"]
#         self.csv = self.info["csv"]
#         self.getout = self.info["getout"]
#         self.check = self.info["check"]
#         self.spreadsheet = self.info["spreadsheet"]
#         self.plot = self.info["plot"]
#
#         ret = str(
#             self.result.set_code(
#                 code=ResultCode.SUCCESS,
#                 msg=logging.info(self.analyzer()),
#             ).data()
#         )
#
#         return ret
#
#
# if __name__ == "__main__":
#     from madup_argo.core.util.date_util import now
#
#     setup_logger()
#     f_path = f"/Users/zane.chang/Downloads/mvp_pjt/athena"
#     worker = AttribModelRunner(dict(), dict())
#     ret = worker.do_work(
#         dict(
#             owner_id="musinsa",
#             product_id="lever",
#             schedule_time=now().replace(tzinfo=None),
#         ),
#         dict(
#             f_path=f_path,
#             mc_with_hmm=False,
#             mc_only=True,
#             basic=False,
#             shap=False,
#             mc_on_hmm=True,
#             redshift=True,
#             csv=False,
#             getout=False,
#             check=False,
#             spreadsheet=False,
#             plot=False,
#         ),
#     )
#
#     print(ret)