import setting.directory as dr
import datetime
import re

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/무신사/DCT237/RD'
paid_dir = raw_dir + '/paid'
organic_dir = raw_dir + '/organic'
report_dir = raw_dir + '/report'
result_dir = dr.download_dir

from_date = datetime.date(2022,7,1)
to_date = datetime.date(2022,9,30)

code_pat = re.compile('[A-Z]{7}\d{3}|[A-Z]{3}\d{1}[A-Z]{3}\d{3}|[A-Z]{4}\d{1}[A-Z]{2}\d{3}|[A-Z]{6}\d{4}')
