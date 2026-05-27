"""Configure and expose the metrics-utility logger and debug helper."""

import functools
import json
import logging
import sys
import time
import uuid
import warnings


if sys.argv[0].endswith('manage.py'):
    warnings.simplefilter(action='ignore', category=FutureWarning)


class JsonFormatter(logging.Formatter):
    """A :class:`logging.Formatter` that emits each log record as a JSON object.

    The emitted JSON object always contains the fields ``timestamp``,
    ``level``, ``logger``, and ``message``.  When the log record carries
    ``collection_id`` or ``duration_ms`` in its ``extra`` dict those fields
    are included as well.

    Example output::

        {"timestamp": "2026-05-27T12:00:00.123456",
         "level": "INFO",
         "logger": "metrics_utility.logger",
         "message": "Analytics collected",
         "collection_id": "a1b2c3d4-...",
         "duration_ms": 142.7}
    """

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: dict = {
            "timestamp": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S.%f"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        if hasattr(record, "collection_id"):
            payload["collection_id"] = record.collection_id  # type: ignore[attr-defined]

        if hasattr(record, "duration_ms"):
            payload["duration_ms"] = record.duration_ms  # type: ignore[attr-defined]

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload)


# Configure the root handler with JSON output.
_handler = logging.StreamHandler()
_handler.setFormatter(JsonFormatter())

# Replace any handlers set up by basicconfig-style initialisation so that the
# entire metrics-utility tree emits structured JSON.
logging.basicConfig(handlers=[_handler], level=logging.INFO)

# note: total_workers_vcpu requires at least INFO
logger = logging.getLogger(__name__)


def debug() -> None:
    """Set the logger level to DEBUG, enabling verbose output."""
    logger.setLevel(logging.DEBUG)


def get_collection_id() -> str:
    """Return a new UUID4 string to use as a correlation ID for one collection run.

    Call this once at the start of a collection run and pass the result as
    ``extra={"collection_id": cid}`` to every :func:`logger` call made during
    that run.  The :class:`JsonFormatter` will include it in each emitted JSON
    object, enabling full correlation across all log lines for a single run.

    Returns:
        A UUID4 string, e.g. ``"a1b2c3d4-e5f6-7890-abcd-ef1234567890"``.
    """
    return str(uuid.uuid4())


def timed(func=None, *, _logger=None):
    """Decorator that measures and logs the execution time of a collector function.

    When the wrapped function returns the decorator emits an INFO log line with
    the fields ``duration_ms`` (elapsed wall-clock time in milliseconds) and
    ``collection_id`` (forwarded from the ``extra`` kwarg when present).

    Usage::

        from metrics_utility.logger import timed, logger

        @timed
        def my_collector(collector, since, until, **kwargs):
            ...

        # Or with an explicit logger:
        @timed(_logger=some_logger)
        def my_collector(...):
            ...

    Args:
        func: The function to wrap (when used as a bare ``@timed`` decorator).
        _logger: Optional :class:`logging.Logger` to use for the timing
            message.  Defaults to the module-level ``logger``.

    Returns:
        The decorated function (or a decorator when called with keyword
        arguments).
    """

    # Support both @timed and @timed(_logger=...) usage.
    if func is None:
        return functools.partial(timed, _logger=_logger)

    log = _logger if _logger is not None else logger

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Capture collection_id from extra kwarg if the caller threads it through.
        extra = kwargs.get("extra") or {}
        cid = extra.get("collection_id")

        start = time.monotonic()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed_ms = (time.monotonic() - start) * 1000.0
            log_extra: dict = {"duration_ms": round(elapsed_ms, 3)}
            if cid:
                log_extra["collection_id"] = cid
            log.info(
                "%(func)s completed in %(ms).1f ms",
                {"func": func.__qualname__, "ms": elapsed_ms},
                extra=log_extra,
            )

    return wrapper
