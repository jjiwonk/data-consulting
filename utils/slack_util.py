import os

from slack import WebClient

from utils.os_util import get_exec_env
from worker.const import DefaultDevAlertSlackChannel, DefaultPrdAlertSlackChannel, DEV, PRD, ResultCode


COLOR_GREEN = "#36a64"
COLOR_ORANGE = "#ff7f00"
COLOR_RED = "#cb2431"


slack_token = os.getenv("SLACK_TOKEN", None)


def send_result_slack_msg(result_code: ResultCode):
    """
    worker의 실행 결과를 슬랙 메시지로 전송.
    환경 변수로 SLACK_TOKEN을 가지고 있지 않으면 아무 작업을 하지 않음.
    """
    if not slack_token:
        return
    slack = WebClient(slack_token)


def get_default_channel(result_code: ResultCode):
    exec_env = get_exec_env()
    default_alert_slack_channel = (
        DefaultPrdAlertSlackChannel if exec_env == PRD else DefaultDevAlertSlackChannel if exec_env == DEV else None
    )
    if default_alert_slack_channel:
        if result_code.value >= ResultCode.ERROR.value:
            return default_alert_slack_channel.ERROR_CH
        elif result_code.value >= ResultCode.WARN.value:
            return default_alert_slack_channel.WARN_CH
        elif result_code.value >= ResultCode.SUCCESS.value:
            return default_alert_slack_channel.SUCCESS_CH
    return None
