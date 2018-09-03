"""Ingest eBird data into the Postgres database."""

from lib.ebird_ingest import EbirdIngest
from lib.db_postgres import DbPostgres


class EbirdIngestPostgres(EbirdIngest):
    """Ingest eBird data into the Postgres database."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    EbirdIngestPostgres(DbPostgres).ingest()
