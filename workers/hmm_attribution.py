import numpy as np
import pandas as pd
import sys, ast, math, datetime as dt

np.set_printoptions(threshold=sys.maxsize)
np.set_printoptions(linewidth=1000)
np.set_printoptions(precision=1)
pd.set_option("display.max_rows", 500)
pd.set_option("display.max_columns", 500)
pd.set_option("display.width", 1000)
dtype = np.float128

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


class MarkovChainAttribution:
    def __init__(self, file, null_exists):
        """
        :param data: a dataframe with path, total_conversions columns
        :param null_exists: whether data's path only have conversion or have conversion and null
        """
        self.file = file
        self.null_exists = null_exists
        self.total_conversion = None
        self.all_channels = None

    def data_preprocessing(self):
        data = self.file.read().splitlines()
        schannels, mchannels = list(), list()
        for row in data[1:]:
            row = ast.literal_eval(row)
            path, conv = ast.literal_eval(row[0]), row[1]
            path = ("start",) + path + ("conv",) if int(conv) >= 1 else ("start",) + path + ("null",)
            for num in range(int(conv)):
                schannels.append(list(path)) if len(path) == 3 else mchannels.append(list(path))

        if not self.null_exists:
            mchannels.append(["start", "madup", "lever", "null"])

        return schannels, mchannels

    def markov_chain(self, channels):
        self.all_channels = list(set([channel for path in channels for channel in path]))
        self.total_conversion = len([path for path in channels if "conv" in path])
        transition = {fstate + ">" + tstate: 0 for fstate in self.all_channels for tstate in self.all_channels}
        for channel in self.all_channels:
            for path in channels:
                if channel in path and channel != "conv" and channel != "null":
                    seq = [i for i, state in enumerate(path) if channel == state]
                    for idx in seq:
                        transition[path[idx] + ">" + path[idx + 1]] += 1

        markov_mat = {state: dict() for state in self.all_channels}
        for channel in self.all_channels:
            if channel != "conv" and channel != "null":
                idx = [i for i, s in enumerate(transition) if s.startswith(channel + ">")]
                total_transition = sum([transition[list(transition)[i]] for i in idx])
                for i in idx:
                    if total_transition > 0:
                        state_prob = float(transition[list(transition)[i]] / total_transition)
                        fstate, tstate = (
                            list(transition)[i].split(">")[0],
                            list(transition)[i].split(">")[1],
                        )
                        markov_mat[fstate][tstate] = state_prob

        markov_mat["conv"]["conv"] = 1.0
        markov_mat["null"]["null"] = 1.0

        return markov_mat

    def removable_effect(self, markov_mat, all_channels):
        if all_channels is not None:
            self.all_channels = all_channels
        markov_mat = pd.DataFrame(markov_mat).transpose().fillna(0.0)
        q_mat = markov_mat.drop(["conv", "null"], axis=0).drop(["conv", "null"], axis=1)
        r_mat = markov_mat[["conv", "null"]].drop(["conv", "null"], axis=0)
        stable_mat = np.dot(np.linalg.inv(np.eye(q_mat.shape[0]) - np.asarray(q_mat)), np.asarray(r_mat))
        all_prob = pd.DataFrame(stable_mat, index=r_mat.index)[[0]].loc["start"].values[0]

        ref_val = dict()
        for channel in self.all_channels:
            if channel != "conv" and channel != "null" and channel != "start":
                removed_mat = markov_mat.drop([channel], axis=0).drop([channel], axis=1)
                q_mat = removed_mat.drop(["conv", "null"], axis=0).drop(["conv", "null"], axis=1)
                r_mat = removed_mat[["conv", "null"]].drop(["conv", "null"], axis=0)
                prob_mat = np.dot(np.linalg.inv(np.eye(q_mat.shape[0]) - q_mat), np.asarray(r_mat))
                re_prob = pd.DataFrame(prob_mat, index=r_mat.index)[[0]].loc["start"].values[0]
                ref_val[channel] = 1.0 - re_prob / all_prob

        return ref_val

    def attribution_distribution(self, ref_val):
        attribution = dict()
        denominator = sum(list(ref_val.values()))
        for channel, ref in ref_val.items():
            if self.total_conversion is not None:
                attribution[channel] = ref / denominator * self.total_conversion
            else:
                attribution[channel] = ref / denominator
        return attribution

    def run(self):
        schannels, mchannels = self.data_preprocessing()
        markov_mat = self.markov_chain(channels=mchannels)
        ref_val = self.removable_effect(markov_mat=markov_mat, all_channels=None)
        attribution = self.attribution_distribution(ref_val=ref_val)

        return attribution


class HiddenMarkovtModel:
    def __init__(self):
        self.file = None
        self.conv_seq = None
        self.markov_mat = None
        self.start_prob = None
        self.emission_mat = None
        self.state = None
        self.time = None
        self.obs = None
        self.backpointer = None
        self.example = False
        self.test_mode = True

    def forward_algorithm(self):
        forward = np.zeros((self.state, self.time), dtype=dtype)
        forward[:, 0] = self.start_prob * self.emission_mat[:, self.conv_seq[0]]
        forward_p = np.zeros((self.state, self.time), dtype=dtype)
        forward_p[:, 0] = self.start_prob * self.emission_mat[:, self.conv_seq[0]]
        for time in range(1, self.time):
            ### for문으로 이해하기 쉬운 코드 ###
            if self.example:
                for state in range(self.state):
                    state_sum = 0.0
                    for before_state in range(self.state):
                        state_sum += (
                            self.markov_mat[before_state, state]
                            * self.emission_mat[state, self.conv_seq[time]]
                            * forward[before_state, time - 1]
                        )
                    forward_p[state, time] = state_sum
            ### numpy로 간소화한 코드 ###
            forward[:, time] = np.dot(
                (self.markov_mat * self.emission_mat[:, self.conv_seq[time]]).transpose(),
                forward[:, time - 1],
            )

        if self.example:
            print(np.linalg.norm(forward) - np.linalg.norm(forward_p))

        return forward

    def backward_algorithm(self):
        backward = np.zeros(shape=(self.state, self.time), dtype=dtype)
        backward[:, self.time - 1] = 1
        backward_p = np.zeros(shape=(self.state, self.time), dtype=dtype)
        backward_p[:, self.time - 1] = 1
        for time in range(self.time - 2, -1, -1):
            ### for문으로 이해하기 쉬운 코드 ###
            if self.example:
                for state in range(self.state):
                    state_sum = 0.0
                    for after_state in range(self.state):
                        state_sum += (
                            self.markov_mat[state, after_state]
                            * self.emission_mat[after_state, self.conv_seq[time + 1]]
                            * backward[after_state, time + 1]
                        )
                    backward_p[state, time] = state_sum
            ### numpy로 간소화한 코드 ###
            backward[:, time] = np.dot(
                self.markov_mat,
                (self.emission_mat[:, self.conv_seq[time + 1]] * backward[:, time + 1]),
            )

        if self.example:
            print(np.linalg.norm(backward) - np.linalg.norm(backward_p))

        return backward

    def posterior_decode(self, forward, backward):
        state_posterior = np.zeros(shape=(self.state, self.time), dtype=dtype)
        state_posterior_p = np.zeros(shape=(self.state, self.time), dtype=dtype)
        trans_posterior = np.zeros(shape=(self.state, self.state, self.time), dtype=dtype)
        trans_posterior_p = np.zeros(shape=(self.state, self.state, self.time), dtype=dtype)
        for time in range(self.time):
            ### for문으로 이해하기 쉬운 코드 ###
            if self.example:
                state_denominator = 0.0
                for state in range(self.state):
                    state_prob = forward[state, time] * backward[state, time]
                    state_posterior_p[state, time] = state_prob
                    state_denominator += state_prob
                for state in range(self.state):
                    if state_denominator > 0:
                        state_posterior_p[state, time] /= state_denominator
            ### numpy로 간소화한 코드 ###
            state_posterior[:, time] = forward[:, time] * backward[:, time]
            if state_posterior[:, time].sum() > 0:
                state_posterior[:, time] /= state_posterior[:, time].sum()

            if time < self.time - 1:
                ### for문으로 이해하기 쉬운 코드 ###
                if self.example:
                    trans_denominator = 0.0
                    for state in range(self.state):
                        for after_state in range(self.state):
                            trans_prob = (
                                self.markov_mat[state, after_state]
                                * self.emission_mat[after_state, self.conv_seq[time + 1]]
                                * forward[state, time]
                                * backward[state, time + 1]
                            )
                            trans_posterior_p[state, after_state, time] = trans_prob
                            trans_denominator += trans_prob
                    for state in range(self.state):
                        for after_state in range(self.state):
                            if trans_denominator > 0:
                                trans_posterior_p[state, after_state, time] /= trans_denominator
                ### numpy로 간소화한 코드 ###
                trans_posterior[:, :, time] = (
                    (self.markov_mat * self.emission_mat[:, self.conv_seq[time + 1]]).transpose()
                    * forward[:, time]
                    * backward[:, time + 1]
                ).transpose()
                if trans_posterior[:, :, time].sum() > 0:
                    trans_posterior[:, :, time] /= trans_posterior[:, :, time].sum()

        if self.test_mode:
            print(
                f"posterior - sum of state: {round(state_posterior.sum(), 0)}, sum of trans: {round(trans_posterior.sum(), 0)} for 1T~{self.time}T"
            )

        if self.example:
            print(np.linalg.norm(state_posterior) - np.linalg.norm(state_posterior_p))
            print(np.linalg.norm(trans_posterior) - np.linalg.norm(trans_posterior_p))

        return state_posterior, trans_posterior

    def expectation_maximization_algorithm(self):
        self.state, self.time, self.obs = (
            self.markov_mat.shape[0],
            len(self.conv_seq),
            self.emission_mat.shape[1],
        )
        self.conv_seq, self.markov_mat, self.start_prob, self.emission_mat = (
            np.asarray([value for key, value in sorted(self.conv_seq.items())]),
            np.asarray(self.markov_mat),
            np.asarray(self.start_prob),
            np.asarray(self.emission_mat),
        )

        cnt, ft = 0, None
        while True:
            before = dt.datetime.now()
            if cnt == 0:
                ft = before
            forward = self.forward_algorithm()
            backward = self.backward_algorithm()
            expectation_state, expectation_trans = self.posterior_decode(
                forward=forward,
                backward=backward,
            )

            maxima_trans = np.zeros((self.state, self.state), dtype=dtype)
            maxima_emission = np.zeros((self.state, self.obs), dtype=dtype)
            maxima_trans_p = np.zeros((self.state, self.state), dtype=dtype)
            maxima_emission_p = np.zeros((self.state, self.obs), dtype=dtype)
            for state in range(self.state):
                ### for문으로 이해하기 쉬운 코드 ###
                if self.example:
                    state_time_sum = 0.0
                    for time in range(self.time - 1):
                        state_time_sum += expectation_state[state, time]
                    for after_state in range(self.state):
                        if state_time_sum > 0:
                            trans_time_sum = 0.0
                            for time in range(self.time - 1):
                                trans_time_sum += expectation_trans[state, after_state, time]
                            maxima_trans_p[state, after_state] = trans_time_sum / state_time_sum

                    state_time_sum += expectation_state[state, self.time - 1]
                    emission_seq_sum = np.asarray([0.0 for i in range(self.obs)])
                    for time in range(self.time):
                        emission_seq_sum[self.conv_seq[time]] += expectation_state[state, time]
                    if state_time_sum > 0:
                        for obs in range(self.obs):
                            maxima_emission_p[state, obs] = emission_seq_sum[obs] / state_time_sum

                ### numpy로 간소화한 코드 ###
                if expectation_state[state, : self.time - 1].sum() > 0:
                    maxima_trans[state, :] = (
                        expectation_trans[state, :, : self.time - 1].sum(axis=1)
                        / expectation_state[state, : self.time - 1].sum()
                    )

                conv_seq = np.asarray(self.conv_seq)
                if expectation_state[state, :].sum() > 0:
                    maxima_emission[state, :] = (
                        np.asarray(
                            [expectation_state[state, np.where(conv_seq == obs)].sum() for obs in range(self.obs)]
                        )
                        / expectation_state[state, :].sum()
                    )

            if self.example:
                print(np.linalg.norm(maxima_trans) - np.linalg.norm(maxima_trans_p))
                print(np.linalg.norm(maxima_emission) - np.linalg.norm(maxima_emission_p))

            after, cnt = dt.datetime.now(), cnt + 1
            if self.test_mode:
                print(
                    f"{cnt} step during {round((after - before).microseconds / 1000000, 2)} sec. whole {round((after - ft).seconds / 60, 2)} min"
                )
                print(
                    f"{maxima_trans.shape} markov mat sum: {round(maxima_trans.sum(), 0)}, {maxima_emission.shape} emission mat sum: {round(maxima_emission.sum(), 0)} on {self.state} states"
                )
                markov_diff = np.format_float_scientific(np.linalg.norm(self.markov_mat - maxima_trans), precision=1)
                emission_diff = np.format_float_scientific(
                    np.linalg.norm(self.emission_mat - maxima_emission), precision=1
                )
                print(f"markov mat diff: {markov_diff}, emission mat diff: {emission_diff}")
            if (
                np.linalg.norm(self.markov_mat - maxima_trans) <= 0.001
                and np.linalg.norm(self.emission_mat - maxima_emission) < 0.001
            ):
                self.markov_mat, self.emission_mat = maxima_trans, maxima_emission
                print(f"elapsed time was {round((after - ft).seconds / 60, 2)} min")
                break
            else:
                self.markov_mat, self.emission_mat = maxima_trans, maxima_emission

    def preprocess(self, data, media_list, start_media):
        for state in range(self.state):
            if self.markov_mat[state, :].sum() > 0:
                self.markov_mat[state, :] = self.markov_mat[state, :] / self.markov_mat[state, :].sum()
        self.markov_mat = np.round(self.markov_mat, 3)
        markov_mat, media_conv, denominator = dict(), dict(), 0
        for media in media_list:
            media_conv[media] = len(
                data[
                    (data["media_source"] == media)
                    & (data["media_path"].apply(lambda x: (ast.literal_eval(x))[1]) == media)
                    & (data["af_purchase"] > 0)
                ]
            )
            conv_scale = 10.0 ** len(str(media_conv[media]))
            if denominator < conv_scale:
                denominator = conv_scale

        for i in range(len(media_list)):
            if not media_list[i] in markov_mat.keys():
                markov_mat[media_list[i]] = dict()
            updated_denominator = denominator + media_conv[media_list[i]]
            for j in range(len(media_list)):
                markov_mat[media_list[i]][media_list[j]] = self.markov_mat[i, j] * denominator / updated_denominator
            markov_mat[media_list[i]]["conv"] = media_conv[media_list[i]] / updated_denominator
        absorb_state = ["start", "conv", "null"]
        for state in absorb_state:
            markov_mat[state] = dict()
            if state == "start":
                for media in media_list:
                    markov_mat[state][media] = start_media[media]
                markov_mat[state][state] = 0
            else:
                markov_mat[state][state] = 1

        return markov_mat

    def optimize(self, use_data_time, file_name):
        df, observation_value = pd.read_csv(file_name), dict()
        if use_data_time:
            df = df[df["media_path"].apply(lambda x: len(ast.literal_eval(x))) == 2]
            df_event_time = df["event_time"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
            min_date = dt.datetime(
                year=df_event_time.min().year,
                month=df_event_time.min().month,
                day=df_event_time.min().day,
                hour=df_event_time.min().hour,
                minute=df_event_time.min().minute,
            )
            max_date = dt.datetime(
                year=df_event_time.max().year,
                month=df_event_time.max().month,
                day=df_event_time.max().day,
                hour=df_event_time.max().hour,
                minute=df_event_time.min().minute,
            )
        else:
            min_date = dt.datetime(year=2022, month=3, day=2, hour=0, minute=1)
            max_date = dt.datetime(year=2022, month=3, day=2, hour=23, minute=1)
            df = df[
                (df["media_path"].apply(lambda x: len(ast.literal_eval(x))) == 2)
                & (df["event_time"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")) >= min_date)
                & (df["event_time"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")) < max_date)
            ]

        print(f"min date: {min_date}, max date: {max_date}")
        df["event_time"] = df["event_time"].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))
        media_list = sorted(set(list(df["media_source"])))

        for i in range((max_date - min_date).days * 24 * 60 + int((max_date - min_date).seconds / 60)):
            sel_df = df[
                (df["event_time"] >= min_date + dt.timedelta(minutes=i))
                & (df["event_time"] < min_date + dt.timedelta(minutes=i + 1))
            ]
            observation_value[min_date + dt.timedelta(minutes=i)] = sel_df["af_purchase"].sum()

        for date, conv in observation_value.items():
            observation_value[date] = 1 if conv > 0 else 0

        if np.sum([val for val in observation_value.values()]) == 0:
            print("There is not any observation in a data")
            exit(1)

        from_media, to_media, markov_mat = dict(), dict(), dict()
        for media_name in media_list:
            from_sel_df = df[df["media_path"].apply(lambda x: (ast.literal_eval(x))[0]) == media_name]
            from_media[media_name], to_media[media_name] = (
                len(from_sel_df.index),
                dict(),
            )
            for transition in media_list:
                to_sel_df = from_sel_df[
                    from_sel_df["media_path"].apply(lambda x: (ast.literal_eval(x))[1]) == transition
                ]
                to_media[media_name][transition] = len(to_sel_df.index)

        for fmedia in media_list:
            markov_mat[fmedia] = dict()
            for tmedia in media_list:
                markov_mat[fmedia][tmedia] = (
                    float(to_media[fmedia][tmedia]) / float(from_media[fmedia]) if float(from_media[fmedia]) > 0 else 0
                )

        start_media = dict()
        for media in media_list:
            start_media[media] = float(from_media[media]) / float(sum(from_media.values()))

        emission_mat = dict()
        for media_name in media_list:
            emission_mat[media_name] = [0, 0]
            sel_df = df[(df["media_source"] == media_name)]
            content_view, purchase = (
                sel_df["af_content_view"].sum(),
                sel_df["af_purchase"].sum(),
            )

            if content_view >= 0:
                emission_mat[media_name][1] = float(purchase) / float(content_view) if content_view > 0 else 0
                emission_mat[media_name][0] = 1 - emission_mat[media_name][1]

            elif content_view == 0 and purchase > 0:
                print("content view is zero while purchase isn't zero")
                exit(1)

        self.conv_seq = observation_value
        self.markov_mat = pd.DataFrame(markov_mat).transpose()
        self.start_prob = list(start_media.values())
        self.emission_mat = pd.DataFrame(emission_mat).transpose()

        if self.example:
            self.conv_seq = ex_conv_seq
            self.markov_mat = ex_markov_mat
            self.start_prob = ex_start_prob
            self.emission_mat = ex_emission_mat

        print(f"num of state: {len(self.markov_mat)}, num of seq: {len(self.conv_seq)}")
        conv_val = [val for val in self.conv_seq.values()]
        for i in range(math.ceil(len(media_list) / 8)):
            print(f"media: {media_list[i*8:(i+1)*8]}")
        for i in range(math.ceil(len(self.conv_seq) / 60)):
            print(f"{i+1} hour: {conv_val[i*60:(i+1)*60]}")
        self.expectation_maximization_algorithm()
        markov_chain = MarkovChainAttribution(file=None, null_exists=None)
        markov_mat = self.preprocess(data=df, media_list=media_list, start_media=start_media)
        ref_val = markov_chain.removable_effect(markov_mat=markov_mat, all_channels=media_list)
        attribution = markov_chain.attribution_distribution(ref_val=ref_val)

        return attribution


if __name__ == "__main__":
    ### Markov Chain Part
    mc_only = False
    if mc_only:
        file_name = "/Users/zane.chang/Downloads/sample_data.csv"
        file = open(file_name, mode="r")
        markov_chain_attribution = MarkovChainAttribution(file=file, null_exists=False)
        markov_chain_res = markov_chain_attribution.run()
        for channel, attribution in markov_chain_res.items():
            print(channel, attribution)

    hmm = HiddenMarkovtModel()
    # file_name = "/Users/zane.chang/Downloads/hmm_id_day_df.csv"
    # file_name = "/Users/zane.chang/Downloads/hmm_id_df_234.csv"
    file_name = "/Users/zane.chang/Downloads/hmm_id_df_0302_0305.csv"
    ret = hmm.optimize(use_data_time=False, file_name=file_name)
    print(ret)
