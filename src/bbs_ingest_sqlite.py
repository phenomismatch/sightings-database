"""Ingest Breed Bird Survey data into the SQLite3 database."""

from lib.bbs_ingest import BbsIngest
from lib.db_sqlite import DbSqlite


class BbsIngestSqlite(BbsIngest):
    """Ingest Breed Bird Survey data into the SQLite3 database."""


if __name__ == '__main__':
    BbsIngestSqlite(DbSqlite).ingest()
