import setting.directory as dr
from solution.DCT283_local_to_googlesheet import raw_data_union as uni
from solution.DCT283_local_to_googlesheet import local_to_gsheet as gsheet


# 실행 전 dcteam@madup-355605.iam.gserviceaccount.com 계정에 편집자 권한 부여 필요
account_name = '핀다'
union_tf = False
update_gsheet_url = 'https://docs.google.com/spreadsheets/d/1AsbLzHn493Btg9AaUtjpBsIc5hWLZLXlpEwGISBtk8E/edit#gid=0'
update_gsheet_name = '시트1'


# 데이터 통합 가공파일 생성
file_name = account_name + '_union_result.xlsx'
raw_dir = dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 솔루션/로컬 to 시트 솔루션/RD/{account_name}'
result_dir = dr.dropbox_dir + f'/광고사업부/데이터컨설팅/데이터 솔루션/로컬 to 시트 솔루션/RD/RESULT/{account_name}'

if union_tf:
    uni.union_raw_data(raw_dir, result_dir, file_name)


# 통합된 데이터 구글 시트로 업로드
token_dir = dr.dropbox_dir + '/광고사업부/데이터컨설팅/token'
client_secret_file = token_dir + '/madup-355605-cd37b0ac201f.json'
update_csv_file = result_dir + '/' + file_name

gsheet.update_gsheet(update_gsheet_url, update_gsheet_name, client_secret_file, update_csv_file)
# gsheet.delete_gsheet(update_gsheet_url, update_gsheet_name, client_secret_file)