import setting.directory as dr
from preprocess.albamon import raw_data

import pyarrow as pa

raw_dir = dr.dropbox_dir + '/광고사업부/4. 광고주/알바몬/4-1. 광고주 제공자료/애드저스트 RAW/7월'
dtypes = {
    '{activity_kind}': pa.string(),
    '{event_name}': pa.string(),
    '{network_name}' : pa.string(),
    '{adid}': pa.string()
}

df = raw_data.adjust_data_read(raw_dir = raw_dir, dtypes = dtypes, len_of_folder=9, media_filter=['Organic'])
