"""Ingest MAPS data into the sqlite database."""

from lib.maps_ingest import MapsIngest
from lib.DbSqlite import DbSqlite


class MapsIngestSqlite(MapsIngest):
    """Ingest MAPS data into the SQLite3 database."""


if __name__ == '__main__':
    MapsIngestSqlite(DbSqlite).ingest()
