"""Ingest Caterpillar Counts data."""


class CaterpillarCountsIngest:
    """Ingest Caterpillar Counts data."""


class CaterpillarCountsIngestPostgres(CaterpillarCountsIngest):
    """Ingest Caterpillar Counts data into the Postgres database."""

    # def _insert_codes(self):
    #     super()._insert_codes()
    #     self.cxn.execute(
    #         f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


class CaterpillarCountsIngestSqlite(CaterpillarCountsIngest):
    """Ingest Caterpillar Counts data into the SQLite3 database."""
