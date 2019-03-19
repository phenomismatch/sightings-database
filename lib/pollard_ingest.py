"""Ingest Pollard data."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util
from lib.util import log


DATASET_ID = 'pollard'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
PLACE_CSV = RAW_DIR / 'Pollard_locations.csv'
DATA_CSV = RAW_DIR / 'pollardbase_example_201802.csv'


def ingest():
    """Ingest the data."""
    raw_data = get_raw_data()

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Pollard lepidoptera observations',
        'version': '2018-02',
        'url': ''})

    to_taxon_id = insert_taxa(raw_data)
    to_place_id = insert_places(raw_data)
    insert_events(raw_data, to_place_id)
    insert_counts(raw_data, to_taxon_id)


def get_raw_data():
    """Read raw data."""
    log(f'Getting {DATASET_ID} raw data')

    raw_data = pd.read_csv(DATA_CSV, dtype='unicode')
    util.normalize_columns_names(raw_data)

    raw_data['started'] = pd.to_datetime(raw_data.Start_time, errors='coerce')

    raw_data['sci_name'] = \
        raw_data.Scientific_Name.str.split().str.join(' ')
    raw_data['dataset_id'] = DATASET_ID

    has_started = raw_data['started'].notna()
    has_sci_name = raw_data['sci_name'].notna()
    raw_data = raw_data.loc[has_started & has_sci_name, :].copy()

    return raw_data


def insert_taxa(raw_data):
    """Insert taxa."""
    log(f'Inserting {DATASET_ID} taxa')

    cxn = db.connect()

    firsts = raw_data['sci_name'].duplicated(keep='first')
    taxa = raw_data.loc[~firsts, ['sci_name', 'Species']]
    taxa.rename(columns={'Species': 'common_name'}, inplace=True)

    taxa['genus'] = taxa['sci_name'].str.split().str[0]
    taxa['class'] = 'lepidoptera'
    taxa['group'] = None
    taxa['order'] = None
    taxa['family'] = None
    taxa['target'] = 't'

    taxa = db.drop_duplicate_taxa(taxa)
    taxa['taxon_id'] = db.get_ids(taxa, 'taxa')
    taxa['taxon_id'] = taxa['taxon_id'].astype(int)
    taxa['taxon_json'] = '{}'

    taxa.to_sql('taxa', cxn, if_exists='append', index=False)

    sql = """SELECT sci_name, taxon_id
               FROM taxa
              WHERE "class" = 'lepidoptera'"""
    return pd.read_sql(sql, cxn).set_index('sci_name').taxon_id.to_dict()


def insert_places(raw_data):
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    raw_places = pd.read_csv(PLACE_CSV, dtype='unicode')
    raw_places.rename(columns=lambda x: x.replace(' ', '_'), inplace=True)

    place_df = raw_data.drop_duplicates(['Site', 'Route']).copy()

    raw_places = pd.merge(
        raw_places, place_df, how='left', on=['Site', 'Route'])

    places = pd.DataFrame()
    raw_places['place_id'] = db.get_ids(raw_places, 'places')
    places['place_id'] = raw_places['place_id']
    places['dataset_id'] = DATASET_ID
    places['lng'] = pd.to_numeric(raw_places['long'], errors='coerce')
    places['lat'] = pd.to_numeric(raw_places['lat'], errors='coerce')
    places['radius'] = None

    fields = ['Site', 'Route', 'County', 'State', 'Land_Owner', 'transect_id',
              'Route_Poin', 'Route_Po_1', 'Route_Po_2', 'CLIMDIV_ID', 'CD_sub',
              'CD_Name', 'ST', 'PRE_MEAN', 'PRE_STD', 'TMP_MEAN', 'TMP_STD']
    places['place_json'] = util.json_object(raw_places, fields)

    places = places[places.lat.notna() & places.lng.notna()]
    places.to_sql('places', db.connect(), if_exists='append', index=False)
    return raw_places.set_index(['Site', 'Route']).place_id.to_dict()


def insert_events(raw_data, to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    events = pd.DataFrame()

    raw_data['event_id'] = db.get_ids(raw_data, 'events')
    events['event_id'] = raw_data['event_id']
    raw_data['place_key'] = tuple(zip(raw_data.Site, raw_data.Route))
    raw_data['place_id'] = raw_data['place_key'].map(to_place_id)
    events['place_id'] = raw_data['place_id']
    events['year'] = raw_data['started'].dt.strftime('%Y')
    events['day'] = raw_data['started'].dt.strftime('%j')
    events['started'] = raw_data['started'].dt.strftime('%H:%M')
    events['ended'] = pd.to_datetime(
        raw_data['End_time'], errors='coerce').dt.strftime('%H:%M')
    events['dataset_id'] = raw_data['dataset_id']

    fields = """
        Site Route County State Start_time End_time Duration Survey Temp
        Sky Wind Archived Was_the_survey_completed Monitoring_Program Date
        Temperature_end Sky_end Wind_end""".split()
    events['event_json'] = util.json_object(raw_data, fields)

    has_place_id = events['place_id'].notna()
    events = events.loc[has_place_id, :]

    events.to_sql('events', db.connect(), if_exists='append', index=False)


def insert_counts(raw_data, to_taxon_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    counts = pd.DataFrame()
    counts['count_id'] = db.get_ids(raw_data, 'counts')
    counts['event_id'] = raw_data['event_id'].astype(int)
    counts['taxon_id'] = raw_data['sci_name'].map(to_taxon_id)
    counts['count'] = raw_data['Total'].fillna(0)
    counts['dataset_id'] = raw_data['dataset_id']

    fields = """A B C D E A_key B_key C_key D_key E_key Observer_Spotter
        Other_participants Recorder_Scribe Taxon_as_reported""".split()
    counts['count_json'] = util.json_object(raw_data, fields)

    has_place_id = raw_data['place_id'].notna().values
    counts = counts.loc[has_place_id, :]

    has_event_id = counts['event_id'].notna()
    has_taxon_id = counts['taxon_id'].notna()
    counts = counts.loc[has_event_id & has_taxon_id, :]

    counts.to_sql('counts', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
