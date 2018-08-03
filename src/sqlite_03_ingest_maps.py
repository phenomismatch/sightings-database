"""Ingest MAPS data into the sqlite database."""

from lib.base_03_ingest_maps import BaseIngestMaps
from lib.sqlite_db import SqliteDb


class SqliteIngestMaps(BaseIngestMaps):
    """Ingest MAPS data into the SQLite3 database."""


if __name__ == '__main__':
    SqliteIngestMaps(SqliteDb).ingest()
