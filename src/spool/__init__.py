"""Spool: crash-safe store-and-forward buffering for edge sensor data."""

from spool.app import Spool
from spool.config import ConfigError
from spool.config import load as load_config
from spool.core.reading import Reading
from spool.core.retention import BufferFull, GapRecord, Policy, Retention

__version__ = "0.1.0.dev0"

__all__ = [
    "BufferFull",
    "ConfigError",
    "GapRecord",
    "Policy",
    "Reading",
    "Retention",
    "Spool",
    "load_config",
]
