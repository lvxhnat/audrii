import logging
import sys

logging.getLogger().setLevel(logging.INFO)

logger = logging.getLogger(__name__)
logger.propagate = False

shell_handler = logging.StreamHandler(sys.stdout)
# file_handler = logging.FileHandler("debug.log")

logger.setLevel(logging.INFO)
shell_handler.setLevel(logging.INFO)
# file_handler.setLevel(logging.WARNING)

fmt_shell = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
fmt_file = (
    "%(levelname)s %(asctime)s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
)

shell_formatter = logging.Formatter(fmt_shell)
file_formatter = logging.Formatter(fmt_file)

shell_handler.setFormatter(shell_formatter)

logger.addHandler(shell_handler)
