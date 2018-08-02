"""Ingest MAPS data into the Postgres database."""

from lib.base_03_ingest_maps import BaseIngestMaps
from lib.postgres_db import PostgresDb


class PostgresIngestMaps(BaseIngestMaps):
    """Ingest MAPS data into the Postgres database."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    PostgresIngestMaps(PostgresDb).ingest()
