import sys
import logging
# https://docs.python.org/3/howto/logging.html#configuring-logging
logger = logging.getLogger(__name__)
logger.propagate = False

shell_handler = logging.StreamHandler(sys.stdout)
# file_handler = logging.FileHandler("debug.log")

logger.setLevel(logging.INFO)
shell_handler.setLevel(logging.INFO)
# file_handler.setLevel(logging.WARNING)

fmt_shell = "[%(asctime)s: %(levelname)s %(funcName)s] %(message)s"

shell_formatter = logging.Formatter(fmt_shell)

shell_handler.setFormatter(shell_formatter)
# file_handler.setFormatter(file_formatter)

logger.addHandler(shell_handler)
# logger.addHandler(file_handler)
