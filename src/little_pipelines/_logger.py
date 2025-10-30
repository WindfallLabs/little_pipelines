import sys

from loguru import logger
from loguru._defaults import LOGURU_FORMAT


logger.remove()

app_fmt = '<light-black>[{time:YYYY-MM-DD HH:mm:ss.SSS}]  </light-black> <level>{message}</level>'
_APP_LOGGER_ID = None

logger.level("APP", no=20, color="<cyan>")
logger.level("PERF", no=20, color="<light-black>")


def make_logger(name, filename, level="DEBUG", retention="30 days", rotation="1 MB", format=LOGURU_FORMAT):
    """
    Create a logger with consistent configuration.
    
    Args:
        name: Identifier for this logger (used in log output and filtering)
        filename: Optional log file path. If provided, logs are written to this file.
                 If None, only logs to stderr.
    
    Returns:
        A bound logger instance configured for this name.
    
    Example:
        >>> my_logger = make_logger("my_module", "logs/my_module.log")
        >>> my_logger.info("Hello world")
    """
    # Create a bound logger with the given name
    bound_logger = logger.bind(logger_name=name)
    if filename is None:
        return bound_logger
    
    bound_logger.add(
        filename,
        format=LOGURU_FORMAT,
        #colorize=True,
        filter=lambda record: record["extra"].get("logger_name") == name,
        rotation=rotation,
        retention=retention,
        level=level
    )
    
    return bound_logger


# App-Level Logger
# Prints all logging messages to the console
# NOTE: any other loggers should only log to files

def make_app_logger(level="INFO"):
    """Handles the global app-level logger."""
    global _APP_LOGGER_ID
    app_logger = logger.bind(name="app_logger")
    _APP_LOGGER_ID = app_logger.add(
        sys.stderr,
        format=app_fmt,
        level=level,
        colorize=True
    )
    return app_logger


# Global app-level logger (console)
app_logger = make_app_logger()


def reset_app_logger(level):
    global app_logger
    logger.remove(_APP_LOGGER_ID)
    app_logger = make_app_logger(level)
    return


__all__ = [
    "make_logger",
    "app_logger",
    "reset_app_logger"
]
