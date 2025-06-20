import logging
from pathlib import PosixPath
from datetime import datetime, timedelta, UTC
from pythonjsonlogger import json as jsonlogger
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
        try:
            return v.relative_to(v.cwd())
        except ValueError:
            return v

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
    level_value = getattr(logging, level.upper(), None)

    assert level_value is not None, f"Invalid log level: {level}"

    if log_format == "json":
        format = "%(message)s"
        formatter = CustomJsonFormatter("%(timestamp)s %(level)s %(name)s %(message)s")

        logHandler = logging.StreamHandler()
        logHandler.setFormatter(formatter)
    elif log_format == "rich":
        format = "%(name)s - %(message)s"
        logHandler = CustomRichHandler(rich_tracebacks=True)
    else:
        raise ValueError(f"Invalid log format: {log_format}")

    logging.basicConfig(
        datefmt="%X",
        force=True,
        format=format,
        handlers=[logHandler],
        level=level_value,
    )

    logging.getLogger("asyncio").setLevel(logging.WARNING)

    logging.captureWarnings(True)

    # # TODO: make this configurable
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)
