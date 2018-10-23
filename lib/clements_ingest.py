"""Extract, transform, & load Clements taxonomy dataset into the database."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util
from lib.log import log


DATASET_ID = 'clements'
TAXON_DIR = Path('data') / 'raw' / 'taxonomy'


def ingest():
    """Extract, transform, & load Clements taxonomy into the database."""
    csv_path = \
        TAXON_DIR / 'eBird-Clements-v2018-integrated-checklist-August-2018.csv'

    db.delete_dataset(DATASET_ID)

    log(f'Ingesting {DATASET_ID} data')

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'version': '2017-07-27',
        'title': 'Standardized birds species codes',
        'url': 'https://www.birdpop.org/pages/birdSpeciesCodes.php'})

    taxons = pd.read_csv(csv_path, dtype='unicode')

    taxons = taxons.rename(columns={
        'scientific name': 'sci_name',
        'English name': 'common_name'})

    is_species = taxons.category == 'species'
    columns = ['sci_name', 'order', 'family', 'common_name']
    taxons = taxons.loc[is_species, columns]

    taxons.sci_name = taxons.sci_name.str.split().str.join(' ')
    taxons['genus'] = taxons.sci_name.str.split().str[0]
    taxons['dataset_id'] = DATASET_ID
    taxons['class'] = 'aves'
    taxons['group'] = None

    targets = pd.read_csv(TAXON_DIR / 'target_birds.csv').sci_name.tolist()
    target = taxons.sci_name.isin(targets)
    taxons.loc[target, 'target'] = 't'

    taxons = util.add_taxon_genera_records(taxons)
    taxons = util.drop_duplicate_taxons(taxons)

    taxons['taxon_id'] = db.get_ids(taxons, 'taxons')
    taxons.to_sql('taxons', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
