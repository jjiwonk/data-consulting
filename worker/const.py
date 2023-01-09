from enum import Enum


PRD = "prd"
DEV = "dev"


class ResultCode(Enum):
    DEBUG = 100
    SUCCESS = 200
    WARN = 300
    ERROR = 400


class DefaultPrdAlertSlackChannel(Enum):
    SUCCESS_CH = "pjt_dc_success"
    WARN_CH = "pjt_dc_warn"
    ERROR_CH = "pjt_dc_error"


class DefaultDevAlertSlackChannel(Enum):
    SUCCESS_CH = "pjt_dc_success_dev"
    WARN_CH = "pjt_dc_warn_dev"
    ERROR_CH = "pjt_dc_error_dev"
