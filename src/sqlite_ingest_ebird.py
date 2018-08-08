"""Ingest eBird data into the SQLite3 database."""

from lib.base_ingest_ebird import BaseIngestEbird
from lib.sqlite_db import SqliteDb


class SqliteIngestEbird(BaseIngestEbird):
    """Ingest eBird data into the SQLite3 database."""


if __name__ == '__main__':
    SqliteIngestEbird(SqliteDb).ingest()
