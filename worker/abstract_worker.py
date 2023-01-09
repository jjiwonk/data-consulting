from abc import ABCMeta, abstractmethod

from utils.os_util import get_exec_env
from utils.slack_util import send_result_slack_msg
from worker.const import ResultCode
from worker.log_handler import logger


class Worker(metaclass=ABCMeta):
    def work(self, info):
        worker_file_name = __file__.split("/")[-1]
        result_code = ResultCode.SUCCESS

        try:
            result = self.do_work(info)
        except Exception as e:
            logger.error(e)
            result_code = ResultCode.ERROR
        send_result_slack_msg(result_code)

    @abstractmethod
    def do_work(self, info):
        pass
