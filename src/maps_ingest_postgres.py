"""Ingest MAPS data into the Postgres database."""

from lib.maps_ingest import MapsIngest
from lib.db_postgres import DbPostgres


class MapsIngestPostgres(MapsIngest):
    """Ingest MAPS data into the Postgres database."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    MapsIngestPostgres(DbPostgres).ingest()
