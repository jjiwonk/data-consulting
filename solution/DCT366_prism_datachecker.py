import spreadsheet.spreadsheet as sp
import pandas as pd

#정합성 체크를 할 데이터 불러오기

def sheet_raw(x): # 변수 이름은 조금 더 직관적인 것을 사용해주시는게 좋을 것 같습니다! x -> sheet_name 요런식으로요
    sheet = sp.spread_document_read(
        'https://docs.google.com/spreadsheets/d/11-TknpD_HzxZ-N7sVEJrS7iEkJcUz4xzJVL0YQPrvZY/edit#gid=0')
    # 함수 내에서 시트를 불러오고 있는데 요렇게 되면 시트를 세번 불러오게 됩니다.
    # 시트 불러오는 부분을 바깥으로 빼내고 시트를 인수로 전달하는 방식이 조금 더 효율적일 것 같아요
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

def col_rule(x): # 요 부분은 간단한 내용이라 함수로 만들지 않아도 될 것 같아요!
    rep_col = df_col['rep']
    api_col = df_col['api']
    col_dic = dict(zip(rep_col,api_col))
    df = x.rename(columns = col_dic)
    return df

df_piv = col_rule(df_piv)
col_name = df_piv.columns.to_list()
df2_piv = df2_piv[col_name]

#데이터간 비교

def compare(df, df2): # iloc 사용하는 경우 연산 속도가 많이 느려져서 아래에 대안적인 방법으로 소개드립니다

    mismat = df.copy()
    for i in range(len(df2.index)):
        for j in range(len(df.columns)):
            if (df2.iloc[i, j] != df.iloc[i, j]):
                mismat.iloc[i, j] = int(df.iloc[i, j]) - int(df2.iloc[i, j])
    return mismat

df_f =compare(df_piv, df2_piv)

# 일부 내용 수정한 ver 2 참고 부탁드립니다

sheet = sp.spread_document_read('https://docs.google.com/spreadsheets/d/11-TknpD_HzxZ-N7sVEJrS7iEkJcUz4xzJVL0YQPrvZY/edit#gid=0')
def sheet_raw_ver2(sheet, sheet_name): # 변수 이름은 조금 더 직관적인 것을 사용해주시는게 좋을 것 같습니다! x -> sheet_name 요런식으로요
    df = sp.spread_sheet(sheet, sheet_name)
    return df

df_col = sheet_raw_ver2(sheet, 'columns')
df_col = df_col.loc[df_col['field']!='none']
column_dict = dict(zip(list(df_col['rep']), list(df_col['api'])))

dimension_list = df_col.loc[df_col['field'] == 'dimension', 'api'].to_list()
metric_list = df_col.loc[df_col['field'] == 'metric', 'api'].to_list()

df = sheet_raw_ver2(sheet, 'RD')
df = df.rename(columns = column_dict)

df2 = sheet_raw_ver2(sheet, 'RD2')
df2[metric_list] = df2[metric_list].apply(lambda x : float(x) * -1) # value 들에 -1 곱해줌

df_concat = pd.concat([df, df2], sort=False, ignore_index= True)
df_pivot = df_concat.pivot_table(index = dimension_list, values = metric_list, aggfunc = 'sum').reset_index() # 데이터 합친다음 피버팅하는데 값이 같았다면 두개 더해서 0이 나올 것




















