import logging
from pathlib import PosixPath
from datetime import datetime, timedelta, UTC
from pythonjsonlogger import jsonlogger
from rich.logging import RichHandler
from pprint import pformat

# TODO: i don't love this
LOGGER = logging.getLogger("app")

RESERVED_ATTRS = [
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
    "message",
]


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    """Basic fields from <https://pypi.org/project/python-json-logger/>."""

    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        if not log_record.get("timestamp"):
            timestamp = datetime.fromtimestamp(record.created, UTC)
            timestamp = timestamp + timedelta(microseconds=record.msecs)

            # TODO: what format?
            log_record["timestamp"] = timestamp
        if log_record.get("level"):
            log_record["level"] = log_record["level"].upper()
        else:
            log_record["level"] = record.levelname


def format_field(v):
    if isinstance(v, datetime):
        return v.isoformat()

    if isinstance(v, PosixPath):
        return v.relative_to(v.cwd())

    return v


class CustomRichHandler(RichHandler):
    def emit(self, record):
        # remove taskName if not set
        if hasattr(record, "taskName") and record.taskName is None:
            del record.taskName

        # Add custom fields to the message, if they exist
        extra = {
            k: format_field(v)
            for k, v in record.__dict__.items()
            if k not in RESERVED_ATTRS
        }
        if len(extra):
            # TODO: rich has a "Pretty" helper but i couldn't easily use it here
            record.msg += f" | {pformat(extra)}"

        super().emit(record)


def setup_logging(level: str, log_format: str):
    level = getattr(logging, level, None)

    assert level is not None, f"Invalid log level: {level}"

    if log_format == "json":
        format = "%(message)s"
        formatter = CustomJsonFormatter("%(timestamp)s %(level)s %(name)s %(message)s")

        logHandler = logging.StreamHandler()
        logHandler.setFormatter(formatter)
    else:
        format = "%(name)s - %(message)s"
        logHandler = CustomRichHandler(rich_tracebacks=True)

    logging.basicConfig(
        format=format,
        datefmt="%X",
        level=level,
        handlers=[logHandler],
    )
