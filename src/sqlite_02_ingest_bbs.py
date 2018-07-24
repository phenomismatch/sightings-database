"""Ingest Breed Bird Survey data into the SQLite3 database."""

from lib.base_02_ingest_bbs import BaseIngestBbs
from lib.sqlite_db import SqliteDb


class SqliteIngestBbs(BaseIngestBbs):
    """Ingest Breed Bird Survey data into the SQLite3 database."""


if __name__ == '__main__':
    SqliteIngestBbs(SqliteDb).ingest()
