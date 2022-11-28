import spreadsheet.spreadsheet as sp
import pandas as pd

#정합성 체크를 할 데이터 불러오기

def sheet_raw(x):
    sheet = sp.spread_document_read(
        'https://docs.google.com/spreadsheets/d/11-TknpD_HzxZ-N7sVEJrS7iEkJcUz4xzJVL0YQPrvZY/edit#gid=0')
    df = sp.spread_sheet(sheet, x)
    return df

df = sheet_raw('RD')
df2 = sheet_raw('RD2')
df_col = sheet_raw('columns')

# 데이터들을 비교하기 위해 피벗팅

def get_pivot(x,y):
    metric = df_col[y].loc[df_col['index'] == 'metric']
    metric = pd.DataFrame(metric)
    s = x.columns.to_list()
    metric = metric.loc[metric[y].isin(s)]
    metric = metric[y].to_list()

    value = df_col[y].loc[df_col['index'] == 'value']
    x[value] = x[value].astype(dtype = 'float64')
    x = x.pivot_table( index = metric , aggfunc = 'sum').reset_index()
    return x

df_piv = get_pivot(df,'rep')
df2_piv = get_pivot(df2,'api')

# api(프리즘) 컬럼 리스트와 매체 컬럼 리스트 정리

def col_rule(x):
    rep_col = df_col['rep']
    api_col = df_col['api']
    col_dic = dict(zip(rep_col,api_col))
    df = x.rename(columns = col_dic)
    return df

df_piv = col_rule(df_piv)
col_name = df_piv.columns.to_list()
df2_piv = df2_piv[col_name]

#데이터간 비교

def compare(df, df2):

    mismat = df.copy()
    for i in range(len(df2.index)):
        for j in range(len(df.columns)):
            if (df2.iloc[i, j] != df.iloc[i, j]):
                mismat.iloc[i, j] = int(df.iloc[i, j]) - int(df2.iloc[i, j])
    return mismat

df_f =compare(df_piv, df2_piv)






















