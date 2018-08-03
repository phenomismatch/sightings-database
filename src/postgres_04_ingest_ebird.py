"""Ingest eBird data into the Postgres database."""

from lib.base_04_ingest_ebird import BaseIngestEbird
from lib.postgres_db import PostgresDb


class PostgresIngestEbird(BaseIngestEbird):
    """Ingest eBird data into the Postgres database."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    PostgresIngestEbird(PostgresDb).ingest()
