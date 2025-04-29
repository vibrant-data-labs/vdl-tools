import logging
import logging.config
from os import path

log_file_path = path.join(path.dirname(path.abspath(__file__)), 'logging.conf')
logging.config.fileConfig(log_file_path)

# create logger
logger = logging.getLogger('web_summarization')
logger.setLevel(logging.INFO)
