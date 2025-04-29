import logging

grey = "\x1b[38;20m"
yellow = "\x1b[33;20m"
red = "\x1b[31;20m"
bold_red = "\x1b[31;1m"
reset = "\x1b[0m"

def info(msg: str):
    logging.info(f'{grey}{msg}{reset}')

def warn(msg: str):
    logging.warn(f'{yellow}{msg}{reset}')

def error(msg: str):
    logging.error(f'{red}{msg}{reset}')
