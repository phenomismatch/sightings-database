"""Ingest Breed Bird Survey data into the Postgres database."""

from lib.base_ingest_bbs import BaseIngestBbs
from lib.postgres_db import PostgresDb


class PostgresIngestBbs(BaseIngestBbs):
    """Ingest Breed Bird Survey data into the Postgres database."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    PostgresIngestBbs(PostgresDb).ingest()
