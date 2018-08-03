"""Ingest Pollard data into the sqlite database."""

from lib.base_05_ingest_pollard import BaseIngestPollard
from lib.sqlite_db import SqliteDb


class SqliteIngestPollard(BaseIngestPollard):
    """Ingest Pollard data into the SQLite3 database."""


if __name__ == '__main__':
    SqliteIngestPollard(SqliteDb).ingest()
