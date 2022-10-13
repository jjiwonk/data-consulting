from analysis.DCT175_finda import prep
from analysis.DCT175_finda import info
from setting import directory as dr
import pandas as pd
import datetime
import numpy as np

raw_df = prep.raw_data_concat(media_filter=['Facebook','Facebook Ads','Facebook_RE_2207','Facebook_MD_2206','Facebook_onelink'],
                         from_date = info.from_date,
                         to_date = info.to_date)

raw_df_exception = prep.campaign_name_exception(raw_df)

raw_data = raw_df_exception.copy()
raw_data['CTET'] = (raw_data['event_time'] - raw_data['attributed_touch_time']).apply(lambda x : x.total_seconds()/86400)
raw_data['CTIT'] = (raw_data['install_time'] - raw_data['attributed_touch_time']).apply(lambda x : x.total_seconds()/86400)
raw_data['ITET'] = (raw_data['event_time'] - raw_data['install_time']).apply(lambda x : x.total_seconds()/86400)
raw_data = raw_data.loc[raw_data['platform']=='android']
raw_data['Cnt'] = 1


loan_data = raw_data.loc[raw_data['event_name'] == 'loan_contract_completed']
loan_data = loan_data.loc[(loan_data['CTIT']<7)&(loan_data['ITET']<30)]
loan_data = loan_data.loc[loan_data['attributed_touch_type']!='impression']

loan_data_source_target = loan_data[['click_weekday', 'event_weekday']]
loan_data_source_target['count'] = 1
loan_data_source_target = loan_data_source_target.rename(columns= {'click_weekday' : 'source', 'event_weekday' : 'target'})
loan_data_source_target = loan_data_source_target.pivot_table(index = ['source', 'target'], values = 'count', aggfunc = 'sum').reset_index()
loan_data_source_target[['source', 'target']] = loan_data_source_target[['source', 'target']].astype('int')
loan_data_source_target['sourceID'] = loan_data_source_target['source']
loan_data_source_target['targetID'] = loan_data_source_target['target']

weekday_dict = {0 : '월',
                1 : '화',
                2 : '수',
                3 : '목',
                4 : '금',
                5 : '토',
                6 : '일'}

loan_data_source_target['source'] = loan_data_source_target['source'].apply(lambda x : weekday_dict.get(x))
loan_data_source_target['target'] = loan_data_source_target['target'].apply(lambda x : weekday_dict.get(x) + '(대출실행)')

loan_data_source_target['targetID'] = loan_data_source_target['targetID'] + 7

import plotly.io as pio
pio.renderers.default='browser'
import plotly.graph_objects as go
import urllib, json
import seaborn as sns


# 라벨 순서 정하기
labelList = ['월', '화', '수', '목', '금', '토', '일', '월(대출실행)','화(대출실행)', '수(대출실행)', '목(대출실행)', '금(대출실행)', '토(대출실행)', '일(대출실행)']

# define colors based on number of levels
df = loan_data_source_target.copy()
cat_cols = loan_data_source_target.columns
value_cols = 'count'

# labelList = []
# labelListTemp = list(set(df_re[cat_cols[0]].values))
#
# for catCol in cat_cols:
#     labelListTemp = list(set(df_re[catCol].values))
#     labelList = labelList + labelListTemp
#
# # remove duplicates from labelList
# labelList = list(dict.fromkeys(labelList))

sourceTargetDf_pivot1 = df.pivot_table(index = 'source', values = 'count', aggfunc='sum')
sourceTargetDf_pivot1 = sourceTargetDf_pivot1.reset_index().rename(columns = {'count' : 'out'})

sourceTargetDf_pivot2 = df.pivot_table(index = 'target', values = 'count', aggfunc='sum')
sourceTargetDf_pivot2 = sourceTargetDf_pivot2.reset_index().rename(columns = {'count' : 'in'})

sourceTargetDf_total = df.merge(sourceTargetDf_pivot1, on ='source', how = 'left')
sourceTargetDf_total = sourceTargetDf_total.merge(sourceTargetDf_pivot2, on ='target', how = 'left')
sourceTargetDf_total['per1'] = (sourceTargetDf_total['count'] / sourceTargetDf_total['out'] * 100).apply(lambda x : str(round(x, 2))+'%')
sourceTargetDf_total['per2'] = (sourceTargetDf_total['count'] / sourceTargetDf_total['in'] * 100).apply(lambda x : str(round(x, 2))+'%')
sourceTargetDf_total['per'] = sourceTargetDf_total['per1'] + ' / ' +  sourceTargetDf_total['per2']


def rgb2hex(color):
    r = int(color[0] * 255)
    g = int(color[1] * 255)
    b = int(color[2] * 255)

    return '#' + hex(r)[2:].zfill(2) + hex(g)[2:].zfill(2) + hex(b)[2:].zfill(2)

color_list = sns.color_palette('Set3', len(labelList)-7)
color_list = [rgb2hex(c) for c in color_list]
color_list = color_list + color_list


sourceTargetDf_total['color'] = sourceTargetDf_total['sourceID'].apply(lambda x : color_list[x])
sourceTargetDf_total = sourceTargetDf_total.sort_values('targetID')
# override gray link colors with 'source' colors
opacity = 0.4
# change 'magenta' to its 'rgba' value to add opacity

fig = go.Figure(data=[go.Sankey(
    valueformat = ",d/f",
    valuesuffix = "",
    # Define nodes
    node = dict(
      pad = 15,
      thickness = 20,
      line = dict(color = "black", width = 0.5),
      label = labelList,
      color = color_list,
        hovertemplate='Event : %{label} <br />Total value : %{value}<extra></extra>',
    ),
    # Add links
    link = dict(
      source = sourceTargetDf_total['sourceID'],
      target = sourceTargetDf_total['targetID'],
      value = sourceTargetDf_total['count'],
      color = sourceTargetDf_total['color'],
        customdata = sourceTargetDf_total['per'],
        hovertemplate='From %{source.label}<br />'+
        'To %{target.label}<br />From &#47; To : %{customdata}<br />Value : %{value}<extra></extra>'
))])
fig.update_layout(title_text="Finda Click To Event Flow",
                  font_size=10)
fig.show()