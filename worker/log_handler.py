from datetime import datetime
import logging
import os

from utils.path_util import get_log_directory


def get_logger(job_name: str):
    logger = logging.getLogger()

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s]  \t(%(filename)s:%(funcName)s:%(lineno)d) %(job_name)s: %(message)s"
    )

    today = datetime.now().strftime("%Y-%m-%d")

    if not os.path.isdir(get_log_directory()):
        os.mkdir(get_log_directory())

    file_handler = logging.FileHandler(f"{get_log_directory()}/{today}.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logging.LoggerAdapter(logger, {"job_name": job_name})
