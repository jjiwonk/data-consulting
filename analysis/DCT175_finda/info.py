import setting.directory as dr
import datetime

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/핀다/DCT175'
paid_dir = raw_dir + '/appsflyer_prism'
organic_dir = raw_dir + '/appsflyer_organic'
result_dir = dr.download_dir

from_date = datetime.date(2022,7,1)
to_date = datetime.date(2022,9,30)