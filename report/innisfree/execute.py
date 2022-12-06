from report.innisfree import merging
from report.innisfree import ref
import setting.directory as dr

import warnings
warnings.filterwarnings("ignore")

integrated_media_df = merging.integrate_media_data()
integrated_media_df.to_csv(dr.download_dir + f'/integrated_media_{ref.report_date.yearmonth}.csv', index=False, encoding='utf-8-sig')
print('download successfully')