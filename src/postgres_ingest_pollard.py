"""Ingest Pollard data into the Postgres database."""

from lib.base_ingest_pollard import BaseIngestPollard
from lib.postgres_db import PostgresDb


class PostgresIngestPollard(BaseIngestPollard):
    """Ingest Pollard data into the Postgres database."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    PostgresIngestPollard(PostgresDb).ingest()
