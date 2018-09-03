"""Ingest NABA data into the sqlite database."""

from lib.naba_ingest import NabaIngest
from lib.db_sqlite import DbSqlite


class NabaIngestSqlite(NabaIngest):
    """Ingest Pollard data into the SQLite3 database."""


if __name__ == '__main__':
    NabaIngestSqlite(DbSqlite).ingest()
