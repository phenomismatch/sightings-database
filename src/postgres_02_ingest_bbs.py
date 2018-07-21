"""Ingest Breed Bird Survey data into the Postgres database."""

from lib.base_02_ingest_bbs import BaseIngestBbs
from lib.postgres import Connection


class PostgresIngestBbs(BaseIngestBbs):
    """Ingest Breed Bird Survey data into the Postgres database."""


if __name__ == '__main__':
    PostgresIngestBbs(Connection).ingest()
