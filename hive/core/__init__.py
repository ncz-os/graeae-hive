"""hive.core — generic infrastructure shared across the hive.

Currently: typed settings (``get_settings``). Adapted from MNEMOS's generic
``core/`` layer — see ``docs/analysis-mnemos-reuse-20261015.md``.
"""

from hive.core import secrets
from hive.core.config import Settings, get_settings

__all__ = ["Settings", "get_settings", "secrets"]
