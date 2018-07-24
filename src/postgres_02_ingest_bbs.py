"""Ingest Breed Bird Survey data into the Postgres database."""

from lib.base_02_ingest_bbs import BaseIngestBbs
from lib.postgres_db import PostgresDb


class PostgresIngestBbs(BaseIngestBbs):
    """Ingest Breed Bird Survey data into the Postgres database."""


if __name__ == '__main__':
    PostgresIngestBbs(PostgresDb).ingest()
