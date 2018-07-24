"""Ingest Pollard data."""

import re
import warnings
from datetime import date
import pandas as pd
import geohash2
import lib.sqlite as db
import lib.data as data


DATASET_ID = 'pollard'
POLLARD_PATH = db.DATA_DIR / 'raw' / DATASET_ID


def ingest_pollard():
    """Ingest Pollard data."""
    warnings.simplefilter(action='ignore', category=UserWarning)

    cxn = db.connect()

    db.delete_dataset(cxn, DATASET_ID)

    pollard = _read_data()
    pollard = _get_species(pollard)
    _insert_dataset(cxn)
    taxons = _insert_taxons(cxn, pollard)
    _insert_records(cxn, pollard, taxons)
    db.upevent_places(cxn, DATASET_ID)


def _read_data():
    print('Reading data')

    places = pd.read_csv(
        POLLARD_PATH / 'Pollard_places.csv', dtype='unicode')
    places = places.rename(
        columns={'lat': 'lat', 'long': 'lng'})
    places = places.drop_duplicates(['Site', 'Route'])

    pollard = pd.read_csv(
        POLLARD_PATH / 'pollardbase_example_201802.csv', dtype='unicode')
    pollard = pollard.rename(columns={
        'Scientific Name': 'sci_name',
        'Species': 'common_name',
        'Total': 'count'})

    pollard['Start time'] = pd.to_datetime(
        pollard['Start time'], errors='coerce')
    pollard = pollard[pollard['Start time'].notna() & pollard.sci_name.notna()]

    pollard = pd.merge(pollard, places, on=['Site', 'Route'], how='left')
    pollard.lat = pd.to_numeric(pollard.lat, errors='coerce')
    pollard.lng = pd.to_numeric(pollard.lng, errors='coerce')
    pollard = pollard[pollard.lat.notna() & pollard.lng.notna()]

    return pollard


def _get_species(pollard):
    parts = pollard.sci_name.str.split(expand=True).drop([2], axis=1).rename(
        columns={0: 'genus', 1: 'combined'})
    pollard = pollard.join(parts)

    parts = pollard.combined.str.split('/', expand=True).rename(
        columns={0: 'species', 1: 'synonym'})
    pollard = pollard.join(parts)

    pollard = pollard[pollard.species.notna()]
    pollard.sci_name = pollard.genus + ' ' + pollard.species

    has_synonym = pollard.synonym.notna()
    pollard.loc[has_synonym, 'synonyms'] = (
        pollard.loc[has_synonym, 'genus']
        + ' '
        + pollard.loc[has_synonym, 'synonym'])

    pollard = pollard.drop(['synonym', 'species', 'combined'], axis=1)
    return pollard


def _insert_dataset(cxn):
    print('Inserting dataset')

    dataset = dict(
        dataset_id=DATASET_ID,
        title='Pollard lepidoptera observations',
        extracted=str(date.today()),
        version='2018-02',
        url='')
    db.insert_dataset(cxn, dataset)


def _insert_taxons(cxn, pollard):
    print('Inserting taxons')

    taxons = pollard.loc[:, ['sci_name', 'common_name']]
    taxons = taxons.drop_duplicates('sci_name')
    taxons['dataset_id'] = DATASET_ID
    taxons['synonyms'] = ''
    taxons['class'] = 'lepidoptera'
    taxons['ordr'] = ''
    taxons['family'] = ''
    taxons['target'] = 1

    taxon_id = db.next_id(cxn, 'taxons')
    taxons['taxon_id'] = range(taxon_id, taxon_id + taxons.shape[0])
    taxons = taxons.set_index('taxon_id')
    taxons.to_sql('taxons', cxn, if_exists='append')
    return taxons.reset_index().set_index('sci_name').taxon_id.to_dict()


def _insert_records(cxn, pollard, taxons):
    print('Inserting event and count records')

    pollard['dataset_id'] = DATASET_ID
    pollard['started'] = pollard['Start time'].dt.strftime('%H:%M:%S')
    pollard['ended'] = pd.to_datetime(
        pollard['End time'], format='%H:%M:%S', errors='coerce')
    pollard['year'] = pollard['Start time'].dt.strftime('%Y')
    pollard['day'] = pollard['Start time'].dt.strftime('%j')
    pollard['radius'] = None
    pollard['geohash'] = pollard.apply(lambda x: geohash2.encode(
        x.lat, x.lng, precision=7), axis=1)
    pollard['taxon_id'] = pollard.sci_name.map(taxons)
    event_id = db.next_id(cxn, 'events')
    pollard['event_id'] = range(event_id, event_id + pollard.shape[0])
    count_id = db.next_id(cxn, 'counts')
    pollard['count_id'] = range(count_id, count_id + pollard.shape[0])

    event_cols = db.EVENT_COLUMNS + re.split(
        r',\s*',
        '''Site, Route, County, State, Land Owner, Duration, Survey, Temp, Sky,
           Wind, Was the survey completed?, Date, Temperature (end), Sky (end),
           Wind (end), Start time, End time, transect_id, Route_Poin,
           Route_Po_1, Route_Po_2, CLIMDIV_ID, CD_sub, CD_Name, ST, PRE_MEAN,
           PRE_STD, TMP_MEAN, TMP_STD''')
    count_cols = db.COUNT_COLUMNS + re.split(
        r',\s*',
        '''A, B, C, D, E, A-key, B-key, C-key, D-key, E-key, Archived,
           Observer/Spotter, Other participants, Recorder/Scribe,
           Monitoring Program, Taxon as reported''')

    pollard = pollard.set_index('event_id')
    data.insert_events(pollard.loc[:, event_cols], cxn, 'pollard_events')

    pollard = pollard.reset_index().set_index('count_id')
    data.insert_counts(pollard.loc[:, count_cols], cxn, 'pollard_counts')


if __name__ == '__main__':
    ingest_pollard()
