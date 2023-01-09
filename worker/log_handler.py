from datetime import datetime
import logging

from utils.path_util import get_root_directory


logger = logging.getLogger()

logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s [%(levelname)s]  \t(%(filename)s:%(funcName)s:%(lineno)d) %(message)s")

today = datetime.now().strftime("%Y-%m%d")
file_handler = logging.FileHandler(f"{get_root_directory()}/logs/{today}.log")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
