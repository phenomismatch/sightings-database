"""Ingest NABA data into the Postgres database."""

from lib.base_ingest_naba import BaseIngestNaba
from lib.postgres_db import PostgresDb


class PostgresIngestNaba(BaseIngestNaba):
    """Ingest NABA data."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    PostgresIngestNaba(PostgresDb).ingest()
