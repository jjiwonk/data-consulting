from analysis.DCT237_musinsa import preparing
from analysis.DCT237_musinsa import preprocess as prep
from analysis.DCT237_musinsa import ltv
from analysis.DCT237_musinsa import info

data_preparing = False

if data_preparing == True :
    preparing.first_purchase_total()
    preparing.cost_data_prep()

cac_data = ltv.acquisition_cost()
cost_data_pivot = cac_data.pivot_table(index = ['계정(Acq)', 'media_source(Acq)'], values = ['cost', 'total_revenue'], aggfunc='sum').reset_index()

ltv_data = ltv.get_ltv_data()

cost_ltv_merge = cost_data_pivot.merge(ltv_data, on = ['계정(Acq)','media_source(Acq)'], how = 'outer')
cost_ltv_merge['CAC'] = cost_ltv_merge['cost'] / cost_ltv_merge['User']
cost_ltv_merge['LTV_estimated'] = cost_ltv_merge['ARPU'] / (1 - cost_ltv_merge['Retention'])
cost_ltv_merge['LTV / CAC'] = cost_ltv_merge['LTV_estimated'] / cost_ltv_merge['CAC']

cost_ltv_merge.to_csv(info.result_dir + '/final_ltv_data_ver2.csv', index=False, encoding = 'utf-8-sig')
print('finished')