import os
dir_list = os.getcwd().split('\\')
# 드롭박스 경로
#dropbox_dir = '/'.join(dir_list[:dir_list.index('Dropbox (주식회사매드업)') + 1])

from setting import directory
dropbox_dir = directory.dropbox_dir

# 다운로드 폴더 경로
download_dir = dropbox_dir + '/광고사업부/4. 광고주/이니스프리/자동화리포트_최종/머징 테스트 파일'
# download_dir = 'C:/Users/MADUP/Downloads'

report_dir = dropbox_dir + '/광고사업부/4. 광고주/이니스프리/자동화리포트'
