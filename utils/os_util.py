import os
import platform


def is_windows_os() -> bool:
    return platform.system() == "Windows"


def is_mac_os() -> bool:
    return platform.system() == "Darwin"


def get_exec_env() -> str:
    return os.getenv("EXEC_ENV", "local")

def clear_folder(file_dir) :
    file_list = os.listdir(file_dir)
    for file in file_list :
        os.remove(file_dir + '/' + file)
