from analysis.DCT237_musinsa import prep
from analysis.DCT237_musinsa import acquisition
from analysis.DCT237_musinsa import ltv
from analysis.DCT237_musinsa import info


prep.first_purchase_total()
prep.cost_data_prep()

cac_data = acquisition.acquisition_cost()
cost_data_pivot = cac_data.pivot_table(index = ['계정(Acq)'], values = 'cost', aggfunc='sum').reset_index()

ltv_data = ltv.get_ltv_data()

cost_ltv_merge = cost_data_pivot.merge(ltv_data, on = '계정(Acq)', how = 'outer')
cost_ltv_merge['CAC'] = cost_ltv_merge['cost'] / cost_ltv_merge['User']
cost_ltv_merge['LTV_estimated'] = cost_ltv_merge['ARPU'] / (1 - cost_ltv_merge['Retention'])
cost_ltv_merge['LTV / CAC'] = cost_ltv_merge['LTV_estimated'] / cost_ltv_merge['CAC']

cost_ltv_merge.to_csv(info.result_dir + '/final_ltv_data.csv', index=False, encoding = 'utf-8-sig')
print('finished')