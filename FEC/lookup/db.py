"""Database connection helpers for lookup service."""

from __future__ import annotations

import os
from contextlib import contextmanager
from functools import lru_cache
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    url = os.environ.get("FEC_LOOKUP_DATABASE_URL")
    if not url:
        raise RuntimeError("FEC_LOOKUP_DATABASE_URL is required")
    pool_size = int(os.environ.get("FEC_LOOKUP_POOL_SIZE", "5"))
    max_overflow = int(os.environ.get("FEC_LOOKUP_MAX_OVERFLOW", "5"))
    return create_engine(
        url,
        pool_size=pool_size,
        max_overflow=max_overflow,
        future=True,
    )


@contextmanager
def connection() -> Iterator:
    engine = get_engine()
    with engine.connect() as conn:
        yield conn
