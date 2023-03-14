import logging
import os
import time

import gspread
import typing
from gspread.utils import finditem
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials

import pandas as pd

from utils.const import *
from utils.path_util import get_tmp_path
from utils.s3 import download_file


RETRY_CNT = 5
WAITING_TIME = 15
GSS_ROW = typing.Dict[str, gspread.Cell]

# 구글 드라이브 사용 시 아래 계정 권한 부여 필요
# dcteam@madup-355605.iam.gserviceaccount.com

class WorkSheet(gspread.worksheet.Worksheet):
    def __init__(self, spreadsheet, properties):
        super().__init__(spreadsheet, properties)

    def get_df_values(self, row: int = 0, col: int = 0) -> pd.DataFrame:
        values = self.get_all_values()
        return pd.DataFrame(values, columns=values[row]).iloc[row + 1:, col:]

    def insert_df(self, data: pd.DataFrame, row: int = 1):
        values = [row.to_list() for row in data.iloc]
        self.insert_rows(values=values, row=row)


class GoogleDrive:
    def __init__(
        self,
        s3_credential_file_path: str = DEFAULT_SPREAD_SHEET_CREDENTIAL_FILE_PATH,
        s3_bucket: str = DEFAULT_S3_PRIVATE_BUCKET,
    ):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        self.__tmp_credential_file_path = get_tmp_path() + "/" + s3_credential_file_path.split("/")[-1]
        credential_file = download_file(s3_path=s3_credential_file_path, s3_bucket=s3_bucket)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file, scope)
        self.google_drive = gspread.authorize(credentials)

    def __del__(self):
        if os.path.exists(self.__tmp_credential_file_path):
            try:
                os.remove(self.__tmp_credential_file_path)
            except Exception as e:
                pass

    def get_spread_sheet(self, url) -> gspread.spreadsheet.Spreadsheet:
        spread_sheet = None
        for i in range(RETRY_CNT):
            try:
                spread_sheet = self.google_drive.open_by_url(url)
                break
            except gspread.exceptions.APIError as e:
                error_code = e.args[0].get("code")
                if error_code == 429 and i != RETRY_CNT - 1:
                    # Call Limit으로 인한 Quota 초과인 경우 대기 후 RETRY_CNT만큼 재시도.
                    time.sleep(WAITING_TIME)
                    continue
                raise e
        return spread_sheet

    def get_work_sheet(self, url, sheet_name) -> WorkSheet:
        spread_sheet = self.get_spread_sheet(url)
        work_sheet = None
        for i in range(RETRY_CNT):
            try:
                sheet_data = spread_sheet.fetch_sheet_metadata()
                item = finditem(
                    lambda x: x["properties"]["title"] == sheet_name,
                    sheet_data["sheets"],
                )
                work_sheet = WorkSheet(spread_sheet, item["properties"])
                break
            except gspread.exceptions.APIError as e:
                error_code = e.args[0].get("code")
                if error_code == 429 and i != RETRY_CNT - 1:
                    # Call Limit으로 인한 Quota 초과인 경우 대기 후 RETRY_CNT만큼 재시도.
                    time.sleep(WAITING_TIME)
                    continue
                raise e
        return work_sheet

    def get_all_rows(self, sheet: gspread.Worksheet, column_list: list = None, header_idx=0) -> typing.List[GSS_ROW]:
        result_data = []
        row_idx = 0
        skip_rows = header_idx + 1
        for row in sheet.get_all_values():
            row_idx += 1
            if skip_rows > 0:
                if skip_rows == 1 and not column_list:
                    column_list = row
                skip_rows -= 1
                continue

            dict_row = dict()
            for col_idx, cell_value in enumerate(row):
                if col_idx >= len(column_list):
                    if cell_value:
                        logging.warning(f"Undefined value[{cell_value}] row[{row_idx}] col[{col_idx}]")
                else:
                    dict_row[column_list[col_idx]] = gspread.Cell(row_idx, col_idx + 1, cell_value)
            result_data.append(dict_row)
        return result_data

    def sheet_to_df(self, sheet: gspread.Worksheet, col_num=0, row_num=0):
        data_read = sheet.get_all_values()
        result = pd.DataFrame(data_read, columns=data_read[row_num]).iloc[row_num + 1:, col_num:]
        return result

    def update_cell(self, sheet, cell: gspread.Cell, value: str, value_input_option: str=None, response_value_render_option: str=None):
        sheet.update(
                   f"{_convert_idx_to_char(cell.col)}{cell.row}",
                   value,
                   value_input_option=value_input_option,
                   response_value_render_option=response_value_render_option)

class GoogleDriveClient:
    def __init__(
            self,
            s3_credential_file_path: str = DEFAULT_SPREAD_SHEET_CREDENTIAL_FILE_PATH,
            s3_bucket: str = DEFAULT_S3_PRIVATE_BUCKET,
    ):
        scope = ['https://www.googleapis.com/auth/drive.metadata',
                  'https://www.googleapis.com/auth/drive.file',
                  'https://www.googleapis.com/auth/drive']
        self.__tmp_credential_file_path = get_tmp_path() + "/" + s3_credential_file_path.split("/")[-1]
        credential_file = download_file(s3_path=s3_credential_file_path, s3_bucket=s3_bucket)
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file, scope)
        self.service = build('drive', 'v3', credentials=credentials)

    def __del__(self):
        if os.path.exists(self.__tmp_credential_file_path):
            try:
                os.remove(self.__tmp_credential_file_path)
            except Exception as e:
                pass

    def file_upload(self, file_paths, name, folder_id):
        gdrive = self.service
        media = MediaFileUpload(file_paths, resumable=True)
        file_metadata = {
            "name": name,
            "parents": [folder_id]}
        return gdrive.files().create(body=file_metadata, media_body=media, fields='id').execute()

def _convert_idx_to_char(idx: int):
    # first column(idx:1) has column label 'A'
    # A's ASCII value is 65
    ret = ''
    idx -= 1
    if idx >= 26:
        tens_idx = int(idx / 26) - 1
        idx = idx % 26
        ret = chr(tens_idx + ord('A'))
    column_ascii_number = idx + ord('A')
    ret += chr(column_ascii_number)
    return ret