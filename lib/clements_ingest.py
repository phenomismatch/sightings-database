"""Extract, transform, & load Clements taxonomy dataset into the database."""

from pathlib import Path
import pandas as pd
import lib.db as db
from lib.util import log


DATASET_ID = 'clements'
TAXON_DIR = Path('data') / 'raw' / 'taxonomy'


def ingest():
    """Extract, transform, & load Clements taxonomy into the database."""
    log(f'Ingesting {DATASET_ID} data')

    csv_path = \
        TAXON_DIR / 'eBird-Clements-v2018-integrated-checklist-August-2018.csv'

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'version': '2017-07-27',
        'title': 'Standardized birds species codes',
        'url': 'https://www.birdpop.org/pages/birdSpeciesCodes.php'})

    taxa = pd.read_csv(csv_path, encoding='ISO-8859-1', dtype=object)

    taxa = taxa.rename(columns={
        'scientific name': 'sci_name',
        'English name': 'common_name',
        'eBird species group': 'group'})

    taxa.sci_name = taxa.sci_name.str.split().str.join(' ')
    taxa = db.drop_duplicate_taxa(taxa)

    taxa['genus'] = taxa.sci_name.str.split().str[0]
    taxa['class'] = 'aves'

    targets = pd.read_csv(TAXON_DIR / 'target_birds.csv').sci_name.tolist()
    targets = taxa.sci_name.isin(targets)
    taxa.loc[targets, 'target'] = 't'

    taxa['taxon_id'] = db.get_ids(taxa, 'taxa')
    taxa.loc[:, db.TAXA_FIELDS].to_sql(
        'taxa', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
