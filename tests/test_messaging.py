# tests/test_messaging.py

import logging

import pytest

from little_pipelines.messaging import (
    DEFAULT_TASK_NAME_LEN,
    FAIL,
    INFO,
    LPFormatter,
    LPLogger,
    TASK_COMPLETE,
    TASK_START,
    Verbosity,
    get_logger,
)


# ============================================================================
# Verbosity
# ============================================================================

@pytest.mark.parametrize(
    ("value", "expected_level", "expected_quiet"),
    [
        ("quiet", Verbosity.QUIET.level, True),
        ("normal", Verbosity.NORMAL.level, False),
        ("verbose", Verbosity.VERBOSE.level, False),
    ],
)
def test_set_verbosity(value, expected_level, expected_quiet):
    logger = LPLogger()

    logger.set_verbosity(value)

    assert logger._logger.level == expected_level
    assert logger.console.quiet is expected_quiet


def test_quiet_property_setter_true():
    logger = LPLogger()

    logger.quiet = True

    assert logger.quiet is True


def test_quiet_property_setter_false():
    logger = LPLogger()

    logger.quiet = False

    assert logger.quiet is False


# ============================================================================
# Configuration
# ============================================================================

def test_set_max_task_name_len():
    logger = LPLogger()

    logger.set_max_task_name_len(42)

    assert logger._task_name_len == 42


def test_default_task_name_len():
    logger = LPLogger()

    assert logger._task_name_len == DEFAULT_TASK_NAME_LEN


# ============================================================================
# Lifecycle
# ============================================================================

def test_start_sets_started_flag():
    logger = LPLogger()

    logger.start()

    try:
        assert logger._started is True
        assert logger._listener is not None
    finally:
        logger.stop()


def test_stop_without_start_is_safe():
    logger = LPLogger()

    logger.stop()

    assert logger._started is False


def test_stop_clears_started_flag():
    logger = LPLogger()

    logger.start()
    logger.stop()

    assert logger._started is False


def test_start_is_idempotent():
    logger = LPLogger()

    logger.start()
    listener = logger._listener

    logger.start()

    try:
        assert logger._listener is listener
    finally:
        logger.stop()


# ============================================================================
# Internal Emission
# ============================================================================

def test_emit_starts_logger_if_needed(monkeypatch):
    logger = LPLogger()

    started = False

    def fake_start(*args, **kwargs):
        nonlocal started
        started = True
        logger._started = True

    monkeypatch.setattr(logger, "start", fake_start)

    calls = []

    def fake_log(level, msg, extra=None):
        calls.append((level, msg, extra))

    monkeypatch.setattr(logger._logger, "log", fake_log)

    logger._emit(logging.INFO, "hello")

    assert started is True
    assert len(calls) == 1


def test_emit_respects_enabled_flag(monkeypatch):
    logger = LPLogger()
    logger.enabled = False

    called = False

    def fake_log(*args, **kwargs):
        nonlocal called
        called = True

    monkeypatch.setattr(logger._logger, "log", fake_log)

    logger._emit(logging.INFO, "hello")

    assert called is False


def test_emit_passes_theme_metadata(monkeypatch):
    logger = LPLogger()
    logger._started = True

    captured = {}

    def fake_log(level, msg, extra=None):
        captured["level"] = level
        captured["msg"] = msg
        captured["extra"] = extra

    monkeypatch.setattr(logger._logger, "log", fake_log)

    logger._emit(
        logging.ERROR,
        "failure",
        task="TaskA",
        theme=FAIL,
    )

    assert captured["level"] == logging.ERROR
    assert captured["msg"] == "failure"
    assert captured["extra"]["task"] == "TaskA"
    assert captured["extra"]["event"] == FAIL.level


# ============================================================================
# Convenience Logging Methods
# ============================================================================

def test_info_uses_info_theme(monkeypatch):
    logger = LPLogger()

    captured = {}

    def fake_emit(level, msg, task, theme):
        captured.update(
            {
                "level": level,
                "msg": msg,
                "task": task,
                "theme": theme,
            }
        )

    monkeypatch.setattr(logger, "_emit", fake_emit)

    logger.info("hello", task="TaskA")

    assert captured["level"] == logging.INFO
    assert captured["theme"] == INFO


def test_warn_uses_warning_level(monkeypatch):
    logger = LPLogger()

    captured = {}

    def fake_emit(level, msg, task, theme):
        captured["level"] = level

    monkeypatch.setattr(logger, "_emit", fake_emit)

    logger.warn("warning")

    assert captured["level"] == logging.WARNING


def test_error_uses_error_level(monkeypatch):
    logger = LPLogger()

    captured = {}

    def fake_emit(level, msg, task, theme):
        captured["level"] = level

    monkeypatch.setattr(logger, "_emit", fake_emit)

    logger.error("boom")

    assert captured["level"] == logging.ERROR


def test_task_start_default_message(monkeypatch):
    logger = LPLogger()

    captured = {}

    def fake_emit(level, msg, task, theme):
        captured["msg"] = msg
        captured["theme"] = theme

    monkeypatch.setattr(logger, "_emit", fake_emit)

    logger.task_start("MyTask")

    assert captured["msg"] == "Running MyTask..."
    assert captured["theme"] == TASK_START


def test_task_start_custom_message(monkeypatch):
    logger = LPLogger()

    captured = {}

    def fake_emit(level, msg, task, theme):
        captured["msg"] = msg

    monkeypatch.setattr(logger, "_emit", fake_emit)

    logger.task_start("MyTask", "custom")

    assert captured["msg"] == "custom"


def test_task_complete_with_elapsed(monkeypatch):
    logger = LPLogger()

    captured = {}

    def fake_emit(level, msg, task, theme):
        captured["msg"] = msg
        captured["theme"] = theme

    monkeypatch.setattr(logger, "_emit", fake_emit)

    logger.task_complete("MyTask", "1.23s")

    assert "(completed in 1.23s)" in captured["msg"]
    assert captured["theme"] == TASK_COMPLETE


def test_task_complete_without_elapsed(monkeypatch):
    logger = LPLogger()

    captured = {}

    def fake_emit(level, msg, task, theme):
        captured["msg"] = msg

    monkeypatch.setattr(logger, "_emit", fake_emit)

    logger.task_complete("MyTask", "")

    assert captured["msg"] == "complete"


# ============================================================================
# Formatter
# ============================================================================

def test_formatter_includes_message():
    formatter = LPFormatter()

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello world",
        args=(),
        exc_info=None,
    )

    output = formatter.format(record)

    assert "hello world" in output
    assert "INFO" in output


def test_formatter_uses_custom_event():
    formatter = LPFormatter()

    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=(),
        exc_info=None,
    )

    record.event = "EXEC"

    output = formatter.format(record)

    assert "EXEC" in output


# ============================================================================
# Singleton
# ============================================================================

def test_get_logger_returns_singleton():
    logger1 = get_logger()
    logger2 = get_logger()

    assert logger1 is logger2
