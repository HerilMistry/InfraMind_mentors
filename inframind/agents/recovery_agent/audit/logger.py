"""Audit logger for the Recovery Agent.
Writes line‑delimited JSON entries to ``logs/recovery_audit.jsonl``.
The logger rotates daily to avoid unbounded file growth.
"""

import json
import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from uuid import uuid4

_LOGGER = None


def get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER:
        return _LOGGER

    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../logs"))
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "recovery_audit.jsonl")

    logger = logging.getLogger("recovery_audit")
    logger.setLevel(logging.INFO)
    handler = TimedRotatingFileHandler(log_path, when="midnight", backupCount=30, utc=True)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    _LOGGER = logger
    return logger


def write_audit_entry(entry: dict) -> str:
    """Write a single audit entry and return its ``audit_id``.

    The ``entry`` dict will be JSON‑encoded on a single line. An ``audit_id``
    (UUID4) is injected if not already present.
    """
    entry = entry.copy()
    audit_id = entry.get("audit_id") or str(uuid4())
    entry["audit_id"] = audit_id
    entry["timestamp"] = datetime.utcnow().isoformat() + "Z"
    logger = get_logger()
    logger.info(json.dumps(entry))
    return audit_id
