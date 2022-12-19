from setting import directory as dr
import pandas as pd
import json
import datetime

raw = pd.read_csv(dr.download_dir+'/SIV_AF브랜드별성과RD_1209_1216.csv')
raw['Attributed Touch Time'] = raw['Attributed Touch Time'].fillna(raw['Install Time'])
raw = raw.dropna(subset=['Install Time'])
raw = raw.loc[raw['Attributed Touch Time'] != 'Attributed Touch Time']

raw['Attributed Touch Time'] = pd.to_datetime(raw['Attributed Touch Time'])
raw['Install Time'] = pd.to_datetime(raw['Install Time'])
raw['Event Time'] = pd.to_datetime(raw['Event Time'])

raw = raw.loc[(raw['Install Time'] - raw['Attributed Touch Time']) < datetime.timedelta(days=1)]
raw = raw.loc[(raw['Event Time'] - raw['Attributed Touch Time']) < datetime.timedelta(days=7)]
raw = raw.loc[(raw['Is Primary Attribution'] == True)|(raw['Is Primary Attribution'] == 'true')]

raw_p = raw.loc[raw['Event Name'] =='completed_purchase']
raw_p['order_id'] = raw_p['Event Value'].apply(lambda x:x.split('af_order_id')[-1].replace('":["', '').replace('"]', '').split(',')[0] if x.find('af_order_id') != -1 else x )

raw_p['중복'] = raw_p.duplicated(['order_id'])

raw_r = raw.loc[raw['Event Name'] =='af_complete_registration']
raw_r['mbr_no'] = raw_r['Event Value'].apply(lambda x:x.split('mbr_no')[-1].replace('":["', '').replace('"]', '').replace('":"', '').replace('"', '').split(',')[0] if x.find('mbr_no') != -1 else x )
raw_r['중복'] = raw_r.duplicated(['mbr_no'])

raw_f = raw.loc[raw['Event Name'] =='first_purchase']
raw_f['order_id'] = raw_f['Event Value'].apply(lambda x:x.split('af_order_id')[-1].replace('":["', '').replace('"]', '').split(',')[0] if x.find('af_order_id') != -1 else x )
raw_f['중복'] = raw_f.duplicated(['order_id'])

raw_c = raw.loc[raw['Event Name'] =='cancel_purchase']
raw_c['order_id'] = raw_c['Event Value'].apply(lambda x:x.split('af_order_id')[-1].replace('":["', '').replace('"]', '').split(',')[0] if x.find('af_order_id') != -1 else x )
raw_c['중복'] = raw_c.duplicated(['order_id'])

df = pd.concat([raw_c,raw_f,raw_r,raw_p])
df['cnt'] = 1
piv =  df.pivot_table(index = 'Event Name', columns='중복', values = 'cnt', aggfunc = 'sum').reset_index().fillna(0)

piv.to_csv(dr.download_dir+'/신세계중복.csv', index= False)