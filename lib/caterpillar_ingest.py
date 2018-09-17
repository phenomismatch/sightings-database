"""Ingest Caterpillar Counts data."""

from datetime import date
import pandas as pd
from lib.util import Caterpillar


class CaterpillarCountsIngest:
    """Ingest Caterpillar Counts data."""

    def __init__(self, db):
        """Setup."""
        self.db = db
        self.cxn = self.db(dataset_id=Caterpillar.dataset_id)

    def ingest(self):
        """Ingest the data."""
        self.cxn.bulk_add_setup()
        self.cxn.delete_dataset()

        raw_sites = self._get_raw_places()
        raw_surveys = self._get_raw_surveys()
        raw_plants = self._get_raw_plants()
        raw_sightings = self._get_raw_sightings()
        to_taxon_id = self._select_taxons()

        self._insert_dataset()
        to_place_id = self._insert_places(raw_sites, raw_data)
        raw_data = self._insert_events(raw_data, to_place_id)
        self._insert_counts(raw_data, to_taxon_id)

        self.cxn.update_places()
        self.cxn.bulk_add_cleanup()

    def _insert_dataset(self):
        print(f'Inserting {Caterpillar.dataset_id} dataset')
        dataset = pd.DataFrame([{
            'dataset_id': Caterpillar.dataset_id,
            'title': 'Caterpillar Counts',
            'extracted': str(date.today()),
            'version': '2018-09-16',
            'url': ('https://caterpillarscount.unc.edu/'
                    'iuFYr1xREQOp2ioB5MHvnCTY39UHv2/')}])
        dataset.set_index('dataset_id').to_sql(
            'datasets', self.cxn.engine, if_exists='append')


class CaterpillarCountsIngestPostgres(CaterpillarCountsIngest):
    """Ingest Caterpillar Counts data into the Postgres database."""


class CaterpillarCountsIngestSqlite(CaterpillarCountsIngest):
    """Ingest Caterpillar Counts data into the SQLite3 database."""
