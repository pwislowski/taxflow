import logging
import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

import structlog

from .config import AppConfig


def add_company_code(_, __, event_dict):
    if "company_code" not in event_dict:
        context = structlog.contextvars.get_contextvars()
        company_code = context.get("company_code")
        if company_code is not None:
            event_dict["company_code"] = company_code
    return event_dict


def configure_logging(config: AppConfig, level: str | None = None):
    """Configure structlog-based logging based on AppConfig."""
    # Map string level to logging constant (fallback to INFO)
    level = level or config.log_level

    # Structlog processors (standard set for structured logging)
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.contextvars.merge_contextvars,
            add_company_code,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Formatters
    console_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.dev.ConsoleRenderer(),
    )
    file_formatter = structlog.stdlib.ProcessorFormatter(
        processor=structlog.processors.JSONRenderer(),
    )

    # Root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers = []  # Clear any existing handlers to avoid duplicates

    # Console handler (if enabled)
    if config.log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    # File handler
    log_dir = Path("logs")
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_id = uuid.uuid4().hex[:8]
    log_filename = log_dir / f"taxflow_{timestamp}_{run_id}.log"
    file_handler = logging.FileHandler(
        filename=log_filename,
        encoding="utf-8",
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    root_logger.propagate = False
