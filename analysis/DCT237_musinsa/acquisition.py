


def acq_data_prep(raw_df, acq_date):
    install_data = raw_df.loc[raw_df['event_name'] == 'install']
    install_data = install_data.loc[pd.to_datetime(install_data['install_time']).dt.date >= acq_date]

    install_user = set(install_data['appsflyer_id'])

    re_data = raw_df.loc[raw_df['event_name'] == 're-engagement']
    re_data = re_data.loc[pd.to_datetime(re_data['install_time']).dt.date >= acq_date]
    re_data = re_data.loc[~(re_data['appsflyer_id'].isin(install_user))]

    acq_data = pd.concat([install_data, re_data], ignore_index=True)
    acq_data['install_time'] = pd.to_datetime(acq_data['install_time'])
    acq_data = acq_data.sort_values('install_time')
    acq_data = acq_data.drop_duplicates('appsflyer_id', keep='first')
    acq_data['campaign'] = acq_data['campaign'].fillna('None')
    acq_data = acq_data.rename(columns={'매체 (Display)': 'acquisition_source', 'campaign': 'acquisition_campaign'})
    return acq_data
