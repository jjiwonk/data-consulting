import setting.directory as dr
import pyarrow as pa
import datetime
import re

raw_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/데이터 분석 프로젝트/W컨셉/DCT512/raw'
paid_dir = raw_dir + '/paid'
organic_dir = raw_dir + '/organic'

dtypes = {
        'install_time': pa.string(),
        'event_time': pa.string(),
        'original_url': pa.string(),
        'attributed_touch_time': pa.string(),
        'media_source': pa.string(),
        'campaign': pa.string(),
        'ad' : pa.string(),
        'event_value': pa.string(),
        'appsflyer_id':pa.string() }