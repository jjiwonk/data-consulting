import pandas as pd
import numpy as np

class BasicAttribution:
    def __init__(self, df, conversion_col_name):
        self.df = df
        self.col = conversion_col_name

    def linear_attribution(self):
        linear_df = self.df.copy()
        linear_df["Path Num"] = linear_df["path"].apply(lambda x: len(x))
        linear_df[self.col] = linear_df.apply(lambda x: x[self.col] / x["Path Num"], axis=1)
        linear_df = linear_df.explode("path")
        linear_df = linear_df.drop(["Path Num"], axis=1)
        attribution = linear_df.groupby("path")[self.col].sum().to_frame()
        attribution[self.col] = attribution[self.col].map(int)
        attribution = attribution.sort_values(by=[self.col], ascending=False)

        return attribution

    def last_click_attribution(self):
        last_click_df = self.df.copy()
        last_click_df["path"] = last_click_df["path"].apply(lambda x: x[-1])
        attribution = last_click_df.groupby("path")[self.col].sum().to_frame()
        attribution = attribution.sort_values(by=[self.col], ascending=False)

        return attribution


class MarkovChainAttribution:
    def __init__(self, df, total_conv, null_exists):
        """
        :param df: a dataframe with path, total_conversions columns
        :param null_exists: whether data's path only have conversion or have conversion and null
        """
        self.df = df
        self.null_exists = null_exists
        self.total_conversion = total_conv
        self.all_channels = None

    def data_preprocessing(self):
        df = self.df
        schannels, mchannels = list(), list()
        for val in df.itertuples():
            path, conv = tuple(val.path), val.conv
            path = ("start",) + path + ("conv",) if int(conv) >= 1 else ("start",) + path + ("null",)
            if int(conv) >= 1:
                for num in range(int(conv)):
                    schannels.append(list(path)) if len(path) == 3 else mchannels.append(list(path))
            else:
                schannels.append(list(path)) if len(path) == 3 else mchannels.append(list(path))

        if not self.null_exists:
            mchannels.append(["start", "madup", "lever", "null"])

        return schannels, mchannels

    def markov_chain(self, channels, media_list):
        if media_list is None:
            self.all_channels = list(set([channel for path in channels for channel in path]))
        else:
            self.all_channels = media_list + ["start", "null", "conv"]
        if self.total_conversion is None:
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
        if not "null" in markov_mat.keys():
            markov_mat["null"] = dict()
        markov_mat["null"]["null"] = 1.0

        return markov_mat

    def removable_effect(self, markov_mat, all_channels, re_file=None):
        if all_channels is not None:
            self.all_channels = all_channels
        markov_mat = pd.DataFrame(markov_mat).transpose().fillna(0.0)
        q_mat = markov_mat.drop(["conv", "null"], axis=0).drop(["conv", "null"], axis=1)
        r_mat = markov_mat[["conv", "null"]].drop(["conv", "null"], axis=0)
        stable_mat = np.dot(np.linalg.inv(np.eye(q_mat.shape[0]) - np.asarray(q_mat)), np.asarray(r_mat))
        all_prob = pd.DataFrame(stable_mat, index=r_mat.index)[[0]].loc["start"].values[0]

        ref_val, re_out = dict(), None
        if re_file is not None:
            re_out = open(file=re_file, mode="w")
            re_out.write(f"account,re_prob,all_prob,prob\n")
        for channel in self.all_channels:
            if channel != "conv" and channel != "null" and channel != "start":
                removed_mat = markov_mat.drop([channel], axis=0).drop([channel], axis=1)
                q_mat = removed_mat.drop(["conv", "null"], axis=0).drop(["conv", "null"], axis=1)
                r_mat = removed_mat[["conv", "null"]].drop(["conv", "null"], axis=0)
                prob_mat = np.dot(np.linalg.inv(np.eye(q_mat.shape[0]) - q_mat), np.asarray(r_mat))
                re_prob = pd.DataFrame(prob_mat, index=r_mat.index)[[0]].loc["start"].values[0]
                ref_val[channel] = 1.0 - re_prob / all_prob

                if re_file is not None:
                    re_out.write(f"{channel},{re_prob},{all_prob},{ref_val[channel]}\n")
        if re_file is not None:
            re_out.close()

        return ref_val

    def attribution_distribution(self, ref_val, mc_file=None):
        attribution, mc_out = dict(), None
        denominator = sum(list(ref_val.values()))
        if mc_file is not None:
            mc_out = open(file=mc_file, mode="w")
            mc_out.write(f"account,attribution")
        for channel, ref in ref_val.items():
            if self.total_conversion is not None:
                attribution[channel] = ref / denominator * self.total_conversion
            else:
                attribution[channel] = ref / denominator

            if mc_file is not None:
                mc_out.write(f"{channel},{attribution[channel]}")
        if mc_file is not None:
            mc_out.close()

        return attribution

    def run(self, media_list, re_file=None, mc_file=None):
        schannels, mchannels = self.data_preprocessing()
        markov_mat = self.markov_chain(channels=mchannels, media_list=media_list)
        ref_val = self.removable_effect(markov_mat=markov_mat, all_channels=None, re_file=re_file)
        attribution = self.attribution_distribution(ref_val=ref_val, mc_file=mc_file)

        return attribution