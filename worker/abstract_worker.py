from abc import ABCMeta, abstractmethod
from datetime import datetime
import os
import traceback

from utils.slack_util import send_result_slack_msg
from worker.const import ResultCode
from worker.log_handler import get_logger


class Worker(metaclass=ABCMeta):
    def __init__(self, job_name):
        self.job_name = os.path.basename(job_name)
        self.logger = get_logger(os.path.basename(self.job_name))
        self.start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def work(self, info: dict, attr: dict = None):
        result = dict()
        alert_channel = None

        try:
            result = self.do_work(info, attr)
            if not isinstance(result, dict):
                result = {"msg": str(result)}
            if not result.get("result_code"):
                result["result_code"] = ResultCode.SUCCESS
            if info.get("slack_channel"):
                alert_channel = info.get("slack_channel")
        except Exception as e:
            tb = traceback.format_exc()
            result["traceback"] = tb
            result["result_code"] = ResultCode.ERROR
            alert_channel = info.get("error_slack_channel")
            self.logger.error(f"{e}\n{tb}")
        try:
            send_result_slack_msg(result, self.job_name, self.start_time, alert_channel)
        except Exception as e:
            self.logger.error(f"슬랙 메시지 전송 실패\n{e}")

    @abstractmethod
    def do_work(self, info: dict, attr: dict) -> dict:
        pass
