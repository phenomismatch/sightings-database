"""Create the SQLite3 database."""

from datetime import datetime
import warnings
import pandas as pd
from lib.base_create_db import BaseCreateDb
from lib.postgres import Connection
import lib.globals as g


class PostgresCreateDb(BaseCreateDb):
    """Create the postgres database and input constant data."""

    def __init__(self):
        """Setup."""
        self.cxn = None

    def create_database(self):
        warnings.simplefilter(action='ignore', category=UserWarning)

        Connection.create()

        self.cxn = Connection()

        _insert_version()
        _insert_countries()
        # _insert_taxons(cxn, datasets)
        # _insert_datasets(cxn, datasets)


def _insert_version(cxn):
    print('Inserting version')
    cxn.execute('INSERT INTO version (version, created) VALUES (%s, %s)',
                ('v0.3', datetime.now()))


def _insert_countries(cxn, datasets):
    print('Inserting countries')

    datasets.append({
        'dataset_id': 'ISO 3166-1',
        'extracted': '2018-01-11',
        'version': '2018-01-11',
        'title': 'ISO 3166-1',
        'url': 'https://en.wikipedia.org/wiki/ISO_3166-1'})

    path = str(g.EXTERNAL / 'misc' / 'ISO_3166-1_country_codes.csv')
    df = pd.read_csv(path)

    df.to_sql(
        'countries', cxn.engine, if_exists='replace', index_label='country_id')
    cxn.execute('ALTER TABLE countries ADD PRIMARY KEY (country_id)')
    cxn.execute('CREATE INDEX countries_code ON countries(code)')
    cxn.execute('CREATE INDEX countries_alpha2 ON countries(alpha2)')
    cxn.execute('CREATE INDEX countries_alpha3 ON countries(alpha3)')


if __name__ == '__main__':
    create_database()
