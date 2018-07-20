"""Create the SQLite3 database."""

import os
import subprocess
from datetime import date
import warnings
import pandas as pd
import lib.sqlite as db


warnings.simplefilter(action='ignore', category=UserWarning)

BIRD_DATASET_ID = 'clem'
EXTERNAL = db.DATA_DIR / 'external'
TAXONOMY = EXTERNAL / 'taxonomy'


def create_database():
    """Create the database and input constant data."""
    if os.path.exists(db.SQLITE_DB):
        os.remove(db.SQLITE_DB)

    print('Creating database')
    subprocess.check_call(db.SQLITE_CREATE, shell=True)

    cxn = db.connect()
    datasets = []

    _insert_version(cxn)
    _insert_countries(cxn, datasets)
    _insert_taxons(cxn, datasets)
    _insert_datasets(cxn, datasets)


def _insert_version(cxn):
    print('Inserting version')
    cxn.execute('INSERT INTO version (version, version_date) VALUES (?, ?)',
                ('v0.2', str(date.today())))
    cxn.commit()


def _insert_datasets(cxn, datasets):
    print('Inserting datasets')
    df = pd.DataFrame(datasets)
    df = df.set_index('dataset_id')
    df = df.to_sql('datasets', cxn, if_exists='append')


def _insert_taxons(cxn, datasets):
    print('Inserting taxons')
    _insert_bird_taxons(cxn, datasets)
    # insert_lep_taxons(cxn, datasets)


def _insert_bird_taxons(cxn, datasets):
    birds = _get_clem_species(datasets)
    birds['dataset_id'] = BIRD_DATASET_ID
    birds['genus'] = birds.sci_name.str.split().str[0]
    birds['class'] = 'aves'
    birds['synonyms'] = ''

    taxon_id = db.next_id(cxn, 'taxons')
    birds['taxon_id'] = range(taxon_id, taxon_id + birds.shape[0])

    birds = birds.rename(columns={'order': 'ordr'}).set_index('taxon_id')
    _set_target_birds(birds)
    birds.to_sql('taxons', cxn, if_exists='append')


def _get_clem_species(datasets):
    path = str(TAXONOMY / 'Clements-Checklist-v2017-August-2017_2.csv')

    datasets.append({
        'dataset_id': BIRD_DATASET_ID,
        'title': 'Standardized birds species codes',
        'extracted': str(date.today()),
        'version': '2017-07-27',
        'url': 'https://www.birdpop.org/pages/birdSpeciesCodes.php'})

    birds = (pd.read_csv(path)
               .rename(columns={'scientific name': 'sci_name',
                                'English name': 'common_name'}))
    is_species = birds.category == 'species'
    birds = birds.loc[is_species,
                      ['sci_name', 'order', 'family', 'common_name']]
    return birds


def _set_target_birds(birds):
    targets = pd.read_csv(TAXONOMY / 'target_birds.csv').sci_name.tolist()
    is_target = birds.sci_name.isin(targets)
    birds.loc[is_target, 'is_target'] = 1


def _insert_countries(cxn, datasets):
    print('Inserting countries')

    path = str(EXTERNAL / 'misc' / 'ISO_3166-1_country_codes.csv')
    datasets.append({
        'dataset_id': 'ISO 3166-1',
        'title': 'ISO 3166-1',
        'extracted': '2018-01-11',
        'version': '2018-01-11',
        'url': 'https://en.wikipedia.org/wiki/ISO_3166-1'})

    (pd.read_csv(path)
       .rename_axis('country_id')
       .to_sql('countries', cxn, if_exists='replace'))
    cxn.execute('CREATE INDEX countries_code ON countries(code)')
    cxn.execute('CREATE INDEX countries_alpha2 ON countries(alpha2)')
    cxn.execute('CREATE INDEX countries_alpha3 ON countries(alpha3)')


if __name__ == '__main__':
    create_database()
