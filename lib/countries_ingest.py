"""Extract, transform & load The ISO Country dataset into a CSV file."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util


DATASET_ID = 'countries'
COUNTRIES_DIR = Path('data') / 'raw' / DATASET_ID
INTERIM_DIR = Path('data') / 'interim'


def ingest():
    """Extract & transform The ISO Country dataset into a CSV file."""
    db.delete_dataset(DATASET_ID)

    util.log(f'Ingesting {DATASET_ID} data')

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'extracted': '2018-01-11',
        'version': '2018-01-11',
        'title': 'ISO 3166-1',
        'url': 'https://en.wikipedia.org/wiki/ISO_3166-1'})

    csv_path = COUNTRIES_DIR / 'ISO_3166-1_country_codes.csv'
    countries = pd.read_csv(csv_path).set_index('code')
    countries.to_sql('countries', db.connect(), if_exists='replace')


if __name__ == "__main__":
    ingest()
