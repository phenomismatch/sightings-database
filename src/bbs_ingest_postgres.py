"""Ingest Breed Bird Survey data into the Postgres database."""

from lib.ingest_bbs import BbsIngest
from lib.db_postgres import DbPostgres


class BbsIngestPostgres(BbsIngest):
    """Ingest Breed Bird Survey data into the Postgres database."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    BbsIngestPostgres(DbPostgres).ingest()