from utils import s3
from utils.path_util import get_tmp_path , get_root_directory

import pandas as pd
import dropbox
import os
from solution.DCT1086_tableau_hyper_upload_tocloud import hyper_file_upload

if __name__ == "__main__":
    worker = hyper_file_upload(__file__)

    def tmp_path_maker(owner_id):
        tmp_path = get_tmp_path() + f"/{owner_id}/"
        os.makedirs(tmp_path, exist_ok=True)
        return tmp_path

    def raw_file_download(owner_id, dropbox_path):
        # 드롭박스에서 raw 데이터 가져오기
        tmp_path = tmp_path_maker(owner_id)
        DROPBOX_ACCESS_TOKEN = os.getenv("DROPBOX_TOKEN", None)

        try:
            dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
            files = dbx.files_list_folder(dropbox_path).entries
            dropbox_list = []

            for file in files:
                if isinstance(file, dropbox.files.FileMetadata):
                    metadata = {'name': file.name}
                    dropbox_list.append(metadata)

            df = pd.DataFrame()

            for i in range(len(dropbox_list)):
                file_name = dropbox_list[i]['name']
                dropbox_file_path = dropbox_path + f'/{file_name}'

                tmp_file = pd.DataFrame()
                tmp_file_path = tmp_path + f'/{file_name}'
                tmp_file.to_csv(tmp_file_path)

                with open(tmp_file_path, 'wb') as f:
                    metadata, result = dbx.files_download(path=dropbox_file_path)
                    f.write(result.content)

                append_df = pd.read_csv(tmp_file_path)
                df = pd.concat([df, append_df])
                os.remove(tmp_file_path)

        except Exception as e:
                print('Error downloading file from Dropbox: ' + str(e))

        return df

    def data_prep( owner_id, dropbox_path ,num_list, double_list, text_list , report_col):
        # 변수 처리 필요
        df = raw_file_download(owner_id,dropbox_path)

        df[['year', 'month', 'date']] = df[['year', 'month', 'date']].fillna(0)
        df[['year','month','date']] = df[['year','month','date']].astype(int)
        # data type 정하기
        df[num_list] = df[num_list].fillna(0)
        df[double_list] = df[double_list].fillna(0)
        df[text_list] = df[text_list].fillna('-')

        df[num_list] = df[num_list].astype(str)
        df[double_list] = df[double_list].astype(str)

        for i in num_list:
            df[i] = df[i].apply(lambda x: x.replace(',', ''))

        for i in double_list:
            df[i] = df[i].apply(lambda x: x.replace(',', ''))

        df[num_list] = df[num_list].astype(float)
        df[double_list] = df[double_list].astype(float)
        df[text_list] = df[text_list].astype(str)

        df['일'] = pd.to_datetime(df['일'])
        df = df.dropna(subset=['일'])
        df = df[report_col]

        df.index = range(len(df))

        data = []
        for i in range(len(df)):
            list = df.loc[i].to_list()
            data.append(list)

        return data

    attr = s3.get_info_from_s3('tableau','hyper_upload_solution_tocloud')

    info = dict(
        hyper_name ='musinsa_report.hyper',
        project_name = 'RD',
        success_alert_channel = 'pjt_dc_success',
        error_slack_channel='pjt_dc_error',
        num_list = [],
        double_list = ['노출', '링크 클릭','광고비_Fee포함','BZ_GGMV(1D)',  'GA_세션', '통합첫구매', '통합구매', '통합매출', '통합사용자', '통합신규사용자', '통합가입','구매', '조회','구매 전환값', '지출 금액 (KRW)','AF_사용자',
       'AF_앱설치_firstopen', 'AF_가입(7D)', 'AF_가입(30D)', 'AF_첫구매(7D)','AF_첫구매(30D)', 'AF_구매(7D)', 'AF_구매(30D)', 'AF_매출(7D)', 'AF_매출(30D)',
       'AF_구매(누적)', 'AF_매출(누적)', 'GA_구매', 'GA_매출', 'GA_가입', 'GA_사용자','GA_사용자(UID)', 'GA_신규사용자', 'GA_첫구매', 'BZ_첫구매(7D)', 'BZ_구매(GGMV/1D)', 'BZ_구매(GGMV/7D)', 'BZ_GGMV(7D)', 'BZ_GMV(1D)','BZ_GMV(7D)'],
        text_list = ['계정 이름', '캠페인 이름', '매체',  '파트', '계정','캠페인/구분', '상세구분','KPI','week', 'day','year', 'month', 'date','광고 세트 이름', '광고명','시작일자', '종료일자','광고 이름','브랜드/소재상세1', '소재상세2', '광고코드','소재형식', '소재내용'],
        date_list = ['일'],
        dropbox_path = '/광고사업부/4. 광고주/무신사/★ 무신사 통합/AWS raw',
    )

    info['os_path'] =  get_root_directory() + '/'+ info['hyper_name']
    info['rd_path'] =  get_root_directory() + '/hyperd.log'
    info['report_col'] =  info['num_list'] + info['double_list'] + info['text_list'] + info['date_list']
    info['data'] = data_prep('musinsa',info['dropbox_path'],info['num_list'],info['double_list'], info['text_list'], info['report_col'])

    worker.work(attr=attr, info=info)
