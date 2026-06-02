"""Shared loguru configuration. Every element calls setup_logging() once at
startup, then imports `logger` from loguru directly elsewhere.

Logs go to two sinks:
  - stderr (human-readable, colorized when a tty)
  - a rotating .log file (path + level from the element's settings)

This is operational logging (lifecycle, connections, dispatch, errors). It is
separate from task `ctx` logging, which is attached to the job result.
"""

import sys
from pathlib import Path

from loguru import logger

_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
    "<level>{level: <8}</level> "
    "<cyan>{extra[element]}</cyan> "
    "<level>{message}</level>"
)

_FILE_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} {level: <8} "
    "{extra[element]} {message}"
)


def setup_logging(element: str, log_file: str, level: str = "INFO"):
    """Configure loguru for one element. Returns a bound logger carrying the
    element name so every line is tagged with which component emitted it."""
    logger.remove()

    logger.configure(extra={"element": element})

    logger.add(
        sys.stderr,
        format=_FORMAT,
        level=level,
        backtrace=True,
        diagnose=False,
    )

    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.add(
        str(path),
        format=_FILE_FORMAT,
        level=level,
        rotation="10 MB",
        retention="7 days",
        enqueue=True,
        backtrace=True,
        diagnose=False,
    )

    return logger
