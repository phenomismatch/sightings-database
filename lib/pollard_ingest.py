"""Ingest Pollard data."""

import re
from pathlib import Path
from datetime import date
import pandas as pd
import lib.db as db
import lib.util as util
from lib.log import log


DATASET_ID = 'pollard'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
PLACE_CSV = RAW_DIR / 'Pollard_locations.csv'
DATA_CSV = RAW_DIR / 'pollardbase_example_201802.csv'


def ingest():
    """Ingest the data."""
    cxn = db.connect()

    raw_data = get_raw_data()

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Pollard lepidoptera observations',
        'extracted': str(date.today()),
        'version': '2018-02',
        'url': ''})

    to_taxon_id = insert_taxons(cxn, raw_data)
    to_place_id = insert_places(cxn, raw_data)
    to_event_id = insert_events(cxn, raw_data, to_place_id)
    # insert_counts(cxn, raw_data, to_event_id, to_taxon_id)


def get_raw_data():
    """Read raw data."""
    log(f'Getting {DATASET_ID} raw data')

    raw_data = pd.read_csv(DATA_CSV, dtype='unicode')
    raw_data.rename(columns=lambda x: re.sub(r'\W', '_', x), inplace=True)

    raw_data['started'] = pd.to_datetime(
        raw_data['Start_time'], errors='coerce')

    raw_data['sci_name'] = \
        raw_data['Scientific_Name'].str.split().str.join(' ')

    raw_data = raw_data[raw_data.started.notna() & raw_data.sci_name.notna()]

    return raw_data


def insert_taxons(cxn, raw_data):
    """Insert taxons."""
    log(f'Inserting {DATASET_ID} taxons')

    firsts = raw_data.sci_name.duplicated(keep='first')
    taxons = raw_data.loc[firsts, ['sci_name', 'Species']]

    taxons.rename(columns={'Species': 'common_name'}, inplace=True)

    taxons['genus'] = taxons.sci_name.str.split().str[0]

    taxons['authority'] = DATASET_ID

    taxons['class'] = 'lepidoptera'
    taxons['group'] = None
    taxons['order'] = None
    taxons['family'] = None
    taxons['target'] = 't'

    taxons = util.add_taxon_genera_records(taxons)
    taxons = util.drop_duplicate_taxons(taxons)
    taxons['taxon_id'] = db.get_ids(taxons, 'taxons')

    taxons.to_sql('taxons', cxn, if_exists='append', index=False)

    return taxons.set_index('sci_name').taxon_id.to_dict()


def insert_places(cxn, raw_data):
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    raw_places = pd.read_csv(PLACE_CSV, dtype='unicode')

    raw_places['radius'] = None
    raw_places.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

    place_df = raw_data.drop_duplicates(['Site', 'Route']).copy()
    place_df.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

    raw_places = pd.merge(
        raw_places, place_df, how='left', on=['Site', 'Route'])

    places = pd.DataFrame()

    raw_places['place_id'] = db.get_ids(raw_places, 'places')
    places['place_id'] = raw_places['place_id']

    places['dataset_id'] = DATASET_ID

    places['lng'] = pd.to_numeric(raw_places['long'], errors='coerce')

    places['lat'] = pd.to_numeric(raw_places['lat'], errors='coerce')

    places['radius'] = None
    places['geohash'] = None

    fields = [
        'Site', 'Route', 'County', 'State', 'Land_Owner', 'transect_id',
        'Route_Poin', 'Route_Po_1', 'Route_Po_2', 'CLIMDIV_ID', 'CD_sub',
        'CD_Name', 'ST', 'PRE_MEAN', 'PRE_STD', 'TMP_MEAN', 'TMP_STD']

    places['place_json'] = util.json_object(raw_places, fields)

    places = places[places.lat.notna() & places.lng.notna()]

    places.to_sql('places', cxn, if_exists='append', index=False)

    return raw_places.set_index(['Site', 'Route']).place_id.to_dict()


def insert_events(cxn, raw_data, to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    events = pd.DataFrame()

    raw_data['ended'] = pd.to_datetime(
        raw_data['End_time'], errors='coerce')

    # event_columns = '''
    #     Site Route County State Start_time End_time Duration Survey Temp
    #     Sky Wind Archived Was_the_survey_completed Monitoring_Program Date
    #     Temperature_end Sky_end Wind_end started ended'''.split()
    # events = raw_data.loc[:, event_columns].copy()
    #
    # events['year'] = events['started'].dt.strftime('%Y')
    # events['day'] = events['started'].dt.strftime('%j')
    # events['started'] = events['started'].dt.strftime('%H:%M:%S')
    # events['ended'] = events['ended'].dt.strftime('%H:%M:%S')
    #
    # events['place_key'] = tuple(zip(events.Site, events.Route))
    # events['place_id'] = events.place_key.map(to_place_id)
    #
    # has_place_id = events.place_id.notna()
    # events = events[has_place_id]
    # raw_data = raw_data[has_place_id].copy()
    #
    # events = events.drop(['place_key'] + self.PLACE_KEYS, axis=1)
    #
    # events = self.cxn.add_event_id(events)
    # raw_data['event_id'] = events.index.values
    #
    # self.cxn.insert_events(events)
    #
    # return raw_data


def insert_counts(cxn, raw_data, to_event_id, to_taxon_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    # count_columns = '''
    #     event_id sci_name count A B C D E A_key B_key C_key D_key E_key
    #     Observer_Spotter Other_participants Recorder_Scribe
    #     Taxon_as_reported'''.split()
    # counts = raw_data.loc[:, count_columns].copy().reset_index()
    #
    # counts['taxon_id'] = counts.sci_name.map(to_taxon_id)
    # counts['count'] = counts['count'].fillna(0)
    # counts.taxon_id = counts.taxon_id.astype(int)
    # counts = self.cxn.add_count_id(counts)
    # counts = counts.drop(['sci_name'], axis='columns')
    # self.cxn.insert_counts(counts)


if __name__ == "__main__":
    ingest()
