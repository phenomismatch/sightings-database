"""Ingest eBird data into the SQLite3 database."""

from lib.ebird_ingest import EbirdIngest
from lib.db_sqlite import DbSqlite


class EbirdIngestSqlite(EbirdIngest):
    """Ingest eBird data into the SQLite3 database."""


if __name__ == '__main__':
    EbirdIngestSqlite(DbSqlite).ingest()
