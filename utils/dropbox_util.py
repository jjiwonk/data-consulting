import os
from sys import getdefaultencoding

import dropbox
from dropbox import dropbox_client
from dropbox.exceptions import ApiError

DEFAULT_TOKEN = os.getenv("DROPBOX_TOKEN", None)
UPLOAD_MAXIMUM_SIZE = 1500000


def get_metadata(path: str, token=DEFAULT_TOKEN):
    dbx = dropbox_client.Dropbox(token)
    path = path.rstrip("/")
    try:
        return dbx.files_get_metadata(path=path)
    except ApiError:
        print("No such path")
        return None


# path가 폴더가 아닌 파일인 경우, 존재하지 않는 경로인 경우 ApiError 발생.
def get_sub_files(path: str, token=DEFAULT_TOKEN):
    dbx = dropbox_client.Dropbox(token)
    path = path.rstrip("/")
    try:
        ls_path = dbx.files_list_folder(path)
    except ApiError:
        print("Error")
        return None
    sub_files = ls_path.entries
    while ls_path.has_more:
        ls_path = dbx.files_list_folder_continue(ls_path.cursor)
        sub_files.extend(ls_path.entries)
    return sub_files


# path가 파일이 아닌 폴더인 경우, 존재하지 않는 경로인 경우 ApiError 발생. TODO 예외처리 추가
def download(path: str, dst=None, token=DEFAULT_TOKEN):
    dbx = dropbox_client.Dropbox(token)
    path = path.rstrip("/")
    if dst:
        meta = dbx.files_download_to_file(download_path=dst, path=path)
        return meta
    else:
        meta, response = dbx.files_download(path=path)
        return meta, response


# 같은 이름으로 존재하는 폴더가 있거나, 경로가 틀린 경우 ApiError.
def create_folder(path: str, autorename=False, token=DEFAULT_TOKEN):
    dbx = dropbox_client.Dropbox(token)
    path = path.rstrip("/")
    dbx.files_create_folder_v2(path=path, autorename=autorename)


# 150MB 이상의 데이터는 upload 세션을 만들어서 진행해야함.
def create_file(path: str, content="", autorename=False, token=DEFAULT_TOKEN):
    dbx = dropbox_client.Dropbox(token)
    if type(content) != bytes:
        content = bytes(content, getdefaultencoding())
    dbx.files_upload(f=content, path=path, autorename=autorename)


def upload_file(file_path: str, dropbox_path: str, autorename=False, token=DEFAULT_TOKEN):
    dbx = dropbox_client.Dropbox(token)
    if os.path.getsize(file_path) > UPLOAD_MAXIMUM_SIZE:
        return
    try:
        content = open(file_path, "r").read()
    except UnicodeDecodeError:
        content = open(file_path, "rb").read()
    create_file(path=dropbox_path, content=content, autorename=autorename)

def upload_v2(
    file_path,
    dropbox_path,
    token=DEFAULT_TOKEN,
    chunk_size=4 * 1024 * 1024,
):
    dbx = dropbox_client.Dropbox(token)
    with open(file_path, "rb") as f:
        file_size = os.path.getsize(file_path)
        if file_size <= chunk_size:
            dbx.files_upload(f.read(), dropbox_path, mode=dropbox.files.WriteMode.overwrite)

        else:
            upload_session_start_result = dbx.files_upload_session_start(
                f.read(chunk_size)
            )
            cursor = dropbox.files.UploadSessionCursor(
                session_id=upload_session_start_result.session_id,
                offset=f.tell(),
            )
            commit = dropbox.files.CommitInfo(
                path=dropbox_path, mode=dropbox.files.WriteMode.overwrite
            )
            while f.tell() < file_size:
                if (file_size - f.tell()) <= chunk_size:
                        dbx.files_upload_session_finish(
                            f.read(chunk_size), cursor, commit
                        )
                else:
                    dbx.files_upload_session_append_v2(
                        f.read(chunk_size), cursor
                    )
                    cursor.offset = f.tell()


# 경로가 틀린 경우 ApiError
def delete(path: str, token=DEFAULT_TOKEN):
    dbx = dropbox_client.Dropbox(token)
    dbx.files_delete_v2(path)
    print(f'deleted: "{path}"')
