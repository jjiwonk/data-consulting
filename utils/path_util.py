import os
from pathlib import Path

from utils.const import EMPTY_STRING


def get_root_directory():
    """data-consulting repository의 root 디렉토리를 반환"""
    return str(Path(__file__).parent.parent.absolute())


def get_tmp_path():
    """tmp 디렉토리 반환"""
    return get_root_directory() + "/tmp"


def get_resource(resource_file: str = EMPTY_STRING):
    """resource 파일명을 전달 받는 경우 resource 파일의 경로를, 아닌 경우에는 resources 디렉토리를 반환"""
    path = f"{get_root_directory()}/resources/{resource_file}"
    if os.path.exists(path):
        return path
    else:
        return None
