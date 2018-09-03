"""Ingest NABA data into the Postgres database."""

from lib.naba_ingest import NabaIngest
from lib.db_postgres import DbPostgres


class NabaIngestPostgres(NabaIngest):
    """Ingest NABA data."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


if __name__ == '__main__':
    NabaIngestPostgres(DbPostgres).ingest()
