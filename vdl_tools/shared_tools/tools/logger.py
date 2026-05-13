import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path, disable_existing_loggers=False)

# create logger
logger = logging.getLogger('vdl_tools')
logger.setLevel(logging.INFO)
