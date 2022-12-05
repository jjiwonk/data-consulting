import setting.directory as dr
import pandas as pd


class dir():
    raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/이니스프리/자동화리포트'
    fb_dir = raw_dir + '/facebook_prism'
    kkm_dir = raw_dir + '/kakaomoment_prism'
    apps_dir = raw_dir + '/appsflyer_prism'
    ga_dir = raw_dir + '/GA'


class cols():
    fb_cols = []
    kkm_cols = []
    apps_cols = []
    ga_cols = []


def get_data(raw_dir, columns, encoding) -> pd.DataFrame:
    raw_df = pd.DataFrame()
    return raw_df

