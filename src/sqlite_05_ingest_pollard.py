"""Ingest Pollard data."""

import warnings
from datetime import date
import pandas as pd
import lib.sqlite as db


DATASET_ID = 'pollard'
POLLARD_PATH = db.DATA_DIR / 'raw' / DATASET_ID


def ingest_pollard():
    """Ingest Pollard data."""
    warnings.simplefilter(action='ignore', category=UserWarning)

    cxn = db.connect()

    db.delete_dataset(cxn, DATASET_ID)

    pollard = read_data()

    insert_dataset(cxn)
    insert_taxons(cxn, pollard)


def read_data():
    path = str(POLLARD_PATH / 'pollardbase_example_201802.xlsx')

    pollard = pd.read_excel(path, dtype='unicode')
    pollard = pollard.loc[:, ['Scientific Name', 'Species']]
    pollard = pollard.rename(
        columns={'Scientific Name': 'sci_name', 'Species': 'common_name'})


def insert_dataset(cxn):
    """Insert a dataset record."""
    print('Inserting dataset')

    dataset = dict(
        dataset_id=DATASET_ID,
        title='Pollard lepidoptera observations',
        extracted=str(date.today()),
        version='2018-02')
    db.insert_dataset(cxn, dataset)


def insert_taxons(cxn, pollard):
    print('Inserting taxons')
    species = get_species(pollard)
    species['class'] = 'lepidoptera'
    species['ordr'] = ''
    species['family'] = ''
    species['is_target'] = 1

    taxon_id = db.next_id(cxn, 'taxons')
    species['taxon_id'] = range(taxon_id, taxon_id + species.shape[0])
    species = species.set_index('taxon_id')
    species.to_sql('taxons', cxn, if_exists='append')


def get_species(pollard):
    dups = pollard.sci_name.duplicated(keep='first')
    species = pollard[~dups]
    species['dataset_id'] = 'pol'
    species['synonyms'] = ''

    parts = species.sci_name.str.split(expand=True).drop([2], axis=1).rename(
        columns={0: 'genus', 1: 'combined'})
    species = species.join(parts)

    parts = species.combined.str.split('/', expand=True).rename(
        columns={0: 'species', 1: 'synonym'})
    species = species.join(parts)

    has_species = species.species.notna()
    species = species[has_species]
    species.sci_name = species.genus + ' ' + species.species
    dups = species.sci_name.duplicated(keep='first')
    species = species[~dups]

    has_synonym = species.synonym.notna()
    species.loc[has_synonym, 'synonyms'] = (
        species.loc[has_synonym, 'genus']
        + ' '
        + species.loc[has_synonym, 'synonym'])

    species = species.drop(['synonym', 'species', 'combined'], axis=1)
    return species


if __name__ == '__main__':
    ingest_pollard()
