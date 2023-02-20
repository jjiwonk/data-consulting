import os

from slack import WebClient

from utils.os_util import get_exec_env
from worker.const import DefaultDevAlertSlackChannel, DefaultPrdAlertSlackChannel, DEV, PRD, ResultCode


COLOR_GREEN = "#36a64f"
COLOR_ORANGE = "#ff7f00"
COLOR_RED = "#cb2431"

SLACK_API_RETRY_CNT = 3


slack_token = os.getenv("SLACK_TOKEN", None)


def send_result_slack_msg(job_result: dict, job_name, start_time, alert_channel):
    """
    worker의 실행 결과를 슬랙 메시지로 전송.
    환경 변수로 SLACK_TOKEN을 가지고 있지 않으면 아무 작업을 하지 않음.
    """
    if not slack_token:
        return
    slack = WebClient(slack_token)

    result_code: ResultCode = job_result.pop("result_code")
    result_df = job_result.pop("result_df")
    result, color = (
        ("ERROR", COLOR_RED) if result_code.value >= ResultCode.ERROR.value else ("SUCCESS", COLOR_GREEN)
    )

    fields = [
        {"type": "mrkdwn", "text": f"*JOB_NAME*\n{job_name}"},
        {"type": "mrkdwn", "text": f"*RESULT*\n{result}"},
        {"type": "mrkdwn", "text": f"*START_TIME*\n{start_time}"},
    ]
    channels = []
    default_channel = get_default_channel(result_code)
    if default_channel:
        channels.append(default_channel)
    if alert_channel:
        fields.append({"type": "mrkdwn", "text": f"*CHANNEL*\n#{alert_channel}"})
        channels.append(alert_channel)

    texts = []
    for k, v in job_result.items():
        texts.append(f"*{k}* => {v}")

    msg = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {"type": "header", "text": {"type": "plain_text", "text": "DC NOTIFICATION"}},
                    {"type": "section", "fields": fields},
                    {"type": "section", "text": {"type": "mrkdwn", "text": "\n".join(texts)}},
                ],
            }
        ]
    }

    exception = None
    for channel in channels:
        for i in range(SLACK_API_RETRY_CNT):
            try:
                slack.chat_postMessage(channel=channel, **msg)
                break
            except Exception as e:
                exception = e
    if exception:
        raise exception


def get_default_channel(result_code: ResultCode):
    exec_env = get_exec_env()
    default_alert_slack_channel = (
        DefaultPrdAlertSlackChannel if exec_env == PRD else DefaultDevAlertSlackChannel if exec_env == DEV else None
    )
    if default_alert_slack_channel:
        if result_code.value >= ResultCode.ERROR.value:
            return default_alert_slack_channel.ERROR_CH.value
        elif result_code.value >= ResultCode.SUCCESS.value:
            return default_alert_slack_channel.SUCCESS_CH.value
    return None
