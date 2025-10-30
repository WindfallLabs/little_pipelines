import sys

from loguru import logger, _Logger
from loguru._defaults import LOGURU_FORMAT


logger.remove()

app_fmt = '<light-black>[{time:YYYY-MM-DD HH:mm:ss.SSS}]  </light-black> <level>{message}</level>'
_APP_LOGGER_ID = None

logger.level("APP", no=20, color="<cyan>")
logger.level("PERF", no=20, color="<light-black>")


def _patch(logger: _Logger) -> _Logger:
    """Custom color-patch."""
    logger.log = logger.opt(colors=True).log
    logger.debug = logger.opt(colors=True).debug
    logger.info = logger.opt(colors=True).info
    logger.warning = logger.opt(colors=True).warning
    logger.error = logger.opt(colors=True).error
    logger.critical = logger.opt(colors=True).critical
    logger.success = logger.opt(colors=True).success

    return logger


def make_logger(
    name,
    filename,
    level="DEBUG",
    colorize=False,
    retention="30 days",
    rotation="1 MB",
    format=LOGURU_FORMAT
) -> _Logger:
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
        format=format,
        colorize=colorize,
        filter=lambda record: record["extra"].get("logger_name") == name,
        rotation=rotation,
        retention=retention,
        level=level
    )

    # Monkey-patched color fixes
    bound_logger = _patch(bound_logger)

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
    # Monkey-patched color fixes
    app_logger = _patch(app_logger)

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
