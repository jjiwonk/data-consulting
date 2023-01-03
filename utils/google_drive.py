import logging
import os
import time

import gspread
import typing
from gspread.utils import finditem
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

from utils.const import *
from utils.path_util import get_tmp_path
from utils.s3 import download_file


RETRY_CNT = 5
WAITING_TIME = 15
GSS_ROW = typing.Dict[str, gspread.Cell]


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
        credential_file = download_file(s3_credential_file_path, s3_bucket)
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