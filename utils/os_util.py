import platform


def is_windows_os() -> bool:
    return platform.system() == "Windows"


def is_mac_os() -> bool:
    return platform.system() == "Darwin"
