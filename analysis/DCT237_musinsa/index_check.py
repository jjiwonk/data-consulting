
def campain_indexing() :
    campaign_list = pd.read_csv(raw_dir + '/musinsa_campaign_list.csv', encoding='utf-8-sig')
    campaign_index = pd.read_csv(raw_dir + '/campaign_index.csv', encoding='utf-8-sig')
    campaign_list = campaign_list['campaign'].dropna(axis=0)
    data = pd.merge(campaign_list, campaign_index, on='campaign', how='left')
    return data

paid_df2 = campain_indexing()

