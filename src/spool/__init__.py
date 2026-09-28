"""Spool: crash-safe store-and-forward buffering for edge sensor data."""

# The single source of the version (pyproject.toml reads it via hatch). Defined before
# the imports so modules imported below can use it.
__version__ = "0.1.0.dev0"

from spool.app import Spool
from spool.config import ConfigError
from spool.config import load as load_config
from spool.core.reading import Reading
from spool.core.retention import BufferFull, GapRecord, Policy, Retention

__all__ = [
    "BufferFull",
    "ConfigError",
    "GapRecord",
    "Policy",
    "Reading",
    "Retention",
    "Spool",
    "__version__",
    "load_config",
]
