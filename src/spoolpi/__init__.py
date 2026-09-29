"""SpoolPi: crash-safe store-and-forward buffering for edge sensor data."""

# The single source of the version (pyproject.toml reads it via hatch). Defined before
# the imports so modules imported below can use it.
__version__ = "0.1.0.dev0"

from spoolpi.app import SpoolPi
from spoolpi.config import ConfigError
from spoolpi.config import load as load_config
from spoolpi.core.reading import Reading
from spoolpi.core.retention import BufferFull, GapRecord, Policy, Retention

__all__ = [
    "BufferFull",
    "ConfigError",
    "GapRecord",
    "Policy",
    "Reading",
    "Retention",
    "SpoolPi",
    "__version__",
    "load_config",
]
