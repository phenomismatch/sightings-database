"""Ingest NABA data into the sqlite database."""

from lib.base_ingest_naba import BaseIngestNaba
from lib.sqlite_db import SqliteDb


class SqliteIngestNaba(BaseIngestNaba):
    """Ingest Pollard data into the SQLite3 database."""


if __name__ == '__main__':
    SqliteIngestNaba(SqliteDb).ingest()
