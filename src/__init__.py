"""site_mon – IBM Mainframe Site Monitoring.

Package entry-point. Exposes only the public interface used by tests
and the container entry-points; internal sub-modules are imported on
demand to keep startup cost low.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("site_mon")
except PackageNotFoundError:  # running from source without install
    __version__ = "0.0.0.dev0"

__all__ = ["__version__"]
