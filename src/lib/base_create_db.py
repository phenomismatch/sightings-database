"""Common logic for creating the database."""

from datetime import datetime
import pandas as pd
import warnings
import lib.globals as g


class BaseCreateDb:
    """Create a generic database and input constant data."""

    CLEMENTS_DATASET_ID = 'clements'

    def __init__(self, cxn):
        """Setup."""
        self.cxn = cxn
        self.datasets = []

    def create_database(self):
        """Create the database and input constant data."""
        print('Creating database')
        warnings.simplefilter(action='ignore', category=UserWarning)

        self.Connection.create()

        self._insert_version()
        self._insert_countries()
        # _insert_taxons(cxn, datasets)
        # _insert_datasets(cxn, datasets)

    def _insert_version(self):
        print('Inserting version')
        self.cxn.execute(
            'INSERT INTO version (version, created) VALUES (?, ?)',
            ('v0.3', str(datetime.now())))

    def _insert_countries(self, datasets):
        print('Inserting countries')

        datasets.append({
            'dataset_id': 'ISO 3166-1',
            'extracted': '2018-01-11',
            'version': '2018-01-11',
            'title': 'ISO 3166-1',
            'url': 'https://en.wikipedia.org/wiki/ISO_3166-1'})

        path = str(g.EXTERNAL / 'misc' / 'ISO_3166-1_country_codes.csv')
        df = pd.read_csv(path)

        df.to_sql('countries', self.cxn.engine, if_exists='replace')
        # self.cxn.execute('ALTER TABLE countries ADD PRIMARY KEY (country_id)')
        self.cxn.execute('CREATE INDEX countries_code ON countries(code)')
        self.cxn.execute('CREATE INDEX countries_alpha2 ON countries(alpha2)')
        self.cxn.execute('CREATE INDEX countries_alpha3 ON countries(alpha3)')
