"""Ingest Pollard data into the sqlite database."""

from lib.pollard_ingest import PollardIngest
from lib.sqlite_db import SqliteDb


class PollardIngestSqlite(PollardIngest):
    """Ingest Pollard data into the SQLite3 database."""


if __name__ == '__main__':
    PollardIngestSqlite(SqliteDb).ingest()
