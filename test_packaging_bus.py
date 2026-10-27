"""The installed distribution must contain the bus and its extracted routers."""

import tomllib
from pathlib import Path


def test_distribution_includes_all_bus_packages():
    root = Path(__file__).parent
    config = tomllib.loads((root / "pyproject.toml").read_text())
    packaged = set(config["tool"]["setuptools"]["packages"])
    bus_packages = {
        ".".join(path.parent.relative_to(root).parts)
        for path in (root / "hive" / "bus").rglob("__init__.py")
    }
    assert bus_packages
    assert bus_packages <= packaged
