"""Fixtures for the persistence contract suite.

One behavioural suite runs against the sqlite reference backend for real and
against any other backend whose DSN is configured. Without a DSN a backend is
SKIPPED (it needs a live server and its driver), never silently passed.

The DSN environment variables are the ones the factory itself reads:
``ORACLE_DSN``, ``POSTGRES_DSN``/``HIVE_PG_DSN``, ``MYSQL_DSN``/``MARIADB_DSN``,
``DB2_DSN``/``HIVE_DB2_DSN``.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from hive.persistence.factory import get_hive_repository

#: backend -> environment variables that would point it at a server.
BACKEND_DSN_ENV = {
    "sqlite": (),
    "postgres": ("POSTGRES_DSN", "HIVE_PG_DSN"),
    "mysql": ("MYSQL_DSN", "MARIADB_DSN"),
    "oracle": ("ORACLE_DSN",),
    "db2": ("DB2_DSN", "HIVE_DB2_DSN"),
}

BACKENDS = list(BACKEND_DSN_ENV)


def _run(coro):
    """Run one async scenario on a fresh loop (no pytest-asyncio needed)."""
    return asyncio.run(coro)


def _make_repo(backend: str):
    if backend == "sqlite":
        tmp = tempfile.TemporaryDirectory()
        repo = get_hive_repository(backend="sqlite", db_path=str(Path(tmp.name) / "contract.db"))
        _run(repo.init())
        return repo, tmp
    repo = get_hive_repository(backend=backend)
    _run(repo.init())
    return repo, None


@pytest.fixture(params=BACKENDS)
def backend(request):
    """A live repository for one backend, or a clean skip without its DSN."""
    name = request.param
    dsn_env = BACKEND_DSN_ENV[name]
    if dsn_env and not any(os.environ.get(var) for var in dsn_env):
        pytest.skip(f"{name}: no DSN configured ({' / '.join(dsn_env)})")
    repo, tmp = _make_repo(name)
    try:
        yield SimpleNamespace(
            name=name,
            repo=repo,
            uid=lambda: uuid.uuid4().hex,
            run=_run,
        )
    finally:
        _run(repo.close())
        if tmp is not None:
            tmp.cleanup()


@pytest.fixture
def only_sqlite(backend):
    """The reference backend: the behavioural suite must always really run here."""
    if backend.name != "sqlite":
        pytest.skip("reference-backend-only check")
    return backend
