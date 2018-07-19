"""Ingest Pollard data."""

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

    pollard = read_data()
    pollard = get_species(pollard)
    insert_dataset(cxn)
    taxons = insert_taxons(cxn, pollard)
    insert_records(cxn, pollard, taxons)
    db.update_points(cxn, DATASET_ID)


def read_data():
    print('Reading data')

    locations = pd.read_csv(
        POLLARD_PATH / 'Pollard_locations.csv', dtype='unicode')
    locations = locations.rename(
        columns={'lat': 'latitude', 'long': 'longitude'})
    locations = locations.drop_duplicates(['Site', 'Route'])

    pollard = pd.read_csv(
        POLLARD_PATH / 'pollardbase_example_201802.csv', dtype='unicode')
    pollard = pollard.rename(columns={
        'Scientific Name': 'sci_name',
        'Species': 'common_name',
        'Land Owner': 'Land_Owner',
        'Start time': 'start_ts',
        'End time': 'end_ts',
        'Total': 'count',
        'A-key': 'A_key',
        'B-key': 'B_key',
        'C-key': 'C_key',
        'D-key': 'D_key',
        'E-key': 'E_key',
        'Observer/Spotter': 'Observer_Spotter',
        'Other participants': 'Other_participants',
        'Recorder/Scribe': 'Recorder_Scribe',
        'Was the survey completed?': 'Was_completed',
        'Monitoring Program': 'Monitoring_Program',
        'Temperature (end)': 'Temperature_end',
        'Sky (end)': 'Sky_end',
        'Wind (end)': 'Wind_end',
        'Taxon as reported': 'Taxon_as_reported'})

    pollard.start_ts = pd.to_datetime(pollard.start_ts, errors='coerce')
    pollard = pollard[pollard.start_ts.notna() & pollard.sci_name.notna()]

    pollard = pd.merge(pollard, locations, on=['Site', 'Route'], how='left')

    pollard.latitude = pd.to_numeric(pollard.latitude, errors='coerce')
    pollard.longitude = pd.to_numeric(pollard.longitude, errors='coerce')
    pollard = pollard[pollard.latitude.notna() & pollard.longitude.notna()]

    return pollard


def get_species(pollard):
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


def insert_dataset(cxn):
    """Insert a dataset record."""
    print('Inserting dataset')

    dataset = dict(
        dataset_id=DATASET_ID,
        title='Pollard lepidoptera observations',
        extracted=str(date.today()),
        version='2018-02',
        url='')
    db.insert_dataset(cxn, dataset)


def insert_taxons(cxn, pollard):
    print('Inserting taxons')

    taxons = pollard.loc[:, ['sci_name', 'common_name']]
    taxons = taxons.drop_duplicates('sci_name')
    taxons['dataset_id'] = DATASET_ID
    taxons['synonyms'] = ''
    taxons['class'] = 'lepidoptera'
    taxons['ordr'] = ''
    taxons['family'] = ''
    taxons['is_target'] = 1

    taxon_id = db.next_id(cxn, 'taxons')
    taxons['taxon_id'] = range(taxon_id, taxon_id + taxons.shape[0])
    taxons = taxons.set_index('taxon_id')
    taxons.to_sql('taxons', cxn, if_exists='append')
    return taxons.reset_index().set_index('sci_name').taxon_id.to_dict()


def insert_records(cxn, pollard, taxons):
    print('Inserting event and count records')

    pollard['dataset_id'] = DATASET_ID
    pollard['start_time'] = pollard.start_ts.dt.strftime('%H:%M:%S')
    pollard['end_time'] = pd.to_datetime(
        pollard.end_ts, format='%H:%M:%S', errors='coerce')
    pollard['year'] = pollard.start_ts.dt.strftime('%Y')
    pollard['day'] = pollard.start_ts.dt.strftime('%j')
    pollard['radius'] = None
    pollard['geohash'] = pollard.apply(lambda x: geohash2.encode(
        x.latitude, x.longitude, precision=7), axis=1)
    pollard['taxon_id'] = pollard.sci_name.map(taxons)
    event_id = db.next_id(cxn, 'events')
    pollard['event_id'] = range(event_id, event_id + pollard.shape[0])
    count_id = db.next_id(cxn, 'counts')
    pollard['count_id'] = range(count_id, count_id + pollard.shape[0])

    event_cols = db.EVENT_COLUMNS + [
        'Site', 'Route', 'County', 'State', 'Land_Owner', 'start_time',
        'end_time', 'Duration', 'Survey', 'Temp', 'Sky', 'Wind', 'transect_id',
        'Route_Poin', 'Route_Po_1', 'Route_Po_2', 'latitude', 'longitude',
        'Date', 'Temperature_end', 'Sky_end', 'Wind_end', 'CLIMDIV_ID',
        'CD_sub', 'CD_Name', 'ST', 'PRE_MEAN', 'PRE_STD', 'TMP_MEAN',
        'TMP_STD', 'Was_completed']
    count_cols = db.COUNT_COLUMNS + [
        'sci_name', 'A', 'B', 'C', 'D', 'E', 'count', 'A_key',
        'B_key', 'C_key', 'D_key', 'E_key', 'Archived', 'Observer_Spotter',
        'Taxon_as_reported', 'Other_participants', 'Recorder_Scribe',
        'Monitoring_Program']

    pollard = pollard.set_index('event_id')
    data.insert_events(pollard.loc[:, event_cols], cxn, 'pollard_events')

    pollard = pollard.reset_index().set_index('count_id')
    data.insert_counts(pollard.loc[:, count_cols], cxn, 'pollard_counts')


if __name__ == '__main__':
    ingest_pollard()
