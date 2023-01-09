from report.innisfree import merging
from report.innisfree import ref
from report.innisfree import tracker_preprocess
from report.innisfree import directory as dr

import warnings
warnings.filterwarnings("ignore")

integrated_df = merging.integrate_data()
integrated_df = merging.final_prep(integrated_df)
integrated_df.to_csv(dr.download_dir + f'/integrated_report_{ref.report_date.yearmonth}.csv', index=False, encoding='utf-8-sig')
print('download successfully')

integrated_media_df = merging.integrate_media_data()
integrated_media_df.to_csv(dr.download_dir + f'/integrated_media_{ref.report_date.yearmonth}.csv', index=False, encoding='utf-8-sig')
print('download successfully')

apps_pivot_df = tracker_preprocess.apps_log_data_prep()
apps_pivot_df.to_csv(dr.download_dir + f'/apps_pivot_{ref.report_date.yearmonth}.csv', index=False, encoding='utf-8-sig')
print('download successfully')

apps_agg_data = tracker_preprocess.get_apps_agg_data()
apps_agg_data.to_csv(dr.download_dir + f'/apps_aggregated_{ref.report_date.yearmonth}.csv', index=False, encoding='utf-8-sig')
print('download successfully')

ga_pivot_df = tracker_preprocess.ga_prep()
ga_pivot_df.to_csv(dr.download_dir + f'/ga_pivot_{ref.report_date.yearmonth}.csv', index=False, encoding='utf-8-sig')
print('download successfully')

# facebook_test = merging.data_merge_test()
# facebook_test.to_csv(dr.download_dir + f'/facebook_merge_test_{ref.report_date.yearmonth}.csv', index=False, encoding='utf-8-sig')
# print('download successfully')

