"""Create the SQLite3 database."""

import os
import subprocess
from pathlib import Path
from datetime import date
import pandas as pd
import lib.sqlite as db


EXTERNAL = Path('data') / 'external'
TAXONOMY = EXTERNAL / 'taxonomy'


def create_database():
    """Create the database and input constant data."""
    if os.path.exists(db.SQLITE_DB):
        os.remove(db.SQLITE_DB)

    print('Creating database')
    subprocess.check_call(db.SQLITE_CREATE, shell=True)

    cxn = db.connect()
    datasets = []

    insert_version(cxn)
    insert_countries(cxn, datasets)
    insert_taxons(cxn, datasets)
    insert_datasets(cxn, datasets)


def insert_version(cxn):
    """Keep a version of the database in the database itself."""
    print('Inserting version')

    cxn.execute('INSERT INTO version (version, version_date) VALUES (?, ?)',
                ('v0.1', str(date.today())))
    cxn.commit()


def insert_datasets(cxn, datasets):
    """Insert dataset records."""
    print('Inserting datasets')

    df = pd.DataFrame(datasets)
    df = df.set_index('dataset_id')
    df = df.to_sql('datasets', cxn, if_exists='append')


def insert_taxons(cxn, datasets):
    """Insert taxon records."""
    print('Inserting taxons')
    insert_bird_taxons(cxn, datasets)
    insert_leps_taxons(cxn, datasets)


def insert_leps_taxons(cxn, datasets):
    """Get the Aves taxon tables."""


def insert_bird_taxons(cxn, datasets):
    """Get the Aves taxon tables."""
    taxon_id = db.next_id(cxn, 'events')
    birds = get_clem_species(datasets)
    birds['genus'] = birds.sci_name.str.split().str[0]
    birds['class'] = 'aves'
    birds['taxon_id'] = range(taxon_id, taxon_id + birds.shape[0])
    birds = birds.rename(columns={'order': 'ordr'}).set_index('taxon_id')
    set_target_birds(birds)
    birds.to_sql('taxons', cxn, if_exists='append')


def get_clem_species(datasets):
    """ETL the IBP species list."""
    path = str(TAXONOMY / 'Clements-Checklist-v2017-August-2017_2.csv')

    datasets.append({
        'dataset_id': 'clem_species',
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


def set_target_birds(birds):
    """Update taxons with target species data."""
    targets = pd.read_csv(TAXONOMY / 'target_birds.csv').sci_name.tolist()
    is_target = birds.sci_name.isin(targets)
    birds.loc[is_target, 'is_target'] = 1


def insert_countries(cxn, datasets):
    """ETL the country codes."""
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
