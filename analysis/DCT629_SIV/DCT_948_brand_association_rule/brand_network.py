import networkx as nx
import pandas as pd

# 가공 완료 된 데이터
ass = pd.read_csv(dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 분석 프로젝트/SIV/result/ass_rule.csv')

ass['antecedents'] =  ass['antecedents'].apply(lambda x: x.replace(']','').replace('[',''))
ass['consequents'] =  ass['consequents'].apply(lambda x: x.replace(']','').replace('[',''))

ass = ass[['antecedents', 'consequents', 'support']]

def network_g(part):
    df = ass.loc[ass['파트'] == part]
    df = df.loc[df['lift'] > 1]
    df.index = range(len(df))

    brand_list = []

    for i in range(len(df)):
        brand1 = df['antecedents'][i]
        brand2 = df['consequents'][i]
        lift = df['lift'][i]
        append_list = tuple([brand1, brand2, lift])
        brand_list.append(append_list)

    G = nx.Graph()
    G.add_weighted_edges_from(brand_list)

    pos = nx.spring_layout(G)  # 각 노드, 엣지를 draw하기 위한 position 정보

    # 노드, 엣지 그리기
    wd = []
    size = 0.1
    for i in range(len(df.index)):
        wd.append(df['lift'].iloc[i] * size)

    nx.draw(G, pos, with_labels=True, font_size=8, node_size=50, width=wd, edge_color='b',
            node_color='b')  # with_labels: 각 노드 이름 표시 여부
