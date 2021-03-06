"""
Ingest nest watch data.

This is data contains observations of the nest status over time. This is
sampled data so the dates of events are approximate. All data is in one file.
"""

from pathlib import Path
from datetime import datetime
import pandas as pd
from . import db
from . import util
from .util import log


DATASET_ID = 'nestwatch'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
DATA_CSV = RAW_DIR / 'Nestwatch_2020_1014.csv'

DATES = 'lay_date hatch_date fledge_date'.split()
EVENT_FIELDS = """LOC_ID CAVITY_ENTRANCE_DIAM_CM FIRST_LAY_DT HATCH_DT
    FLEDGE_DT ENTRANCE_ORIENTATION HABITAT_CODE_1 HABITAT_CODE_2
    HABITAT_CODE_3 PROJ_PERIOD_ID USER_ID OUTCOME_CODE_LIST ATTEMPT_ID
    place_id event_type""".split()
COUNT_FIELDS = "SPECIES_CODE taxon_id count_type""".split()
NUMBERS = """CLUTCH_SIZE_HOST_ATLEAST EGGS_HOST_UNH_ATLEAST
    YOUNG_HOST_TOTAL_ATLEAST YOUNG_HOST_FLEDGED_ATLEAST
    YOUNG_HOST_DEAD_ATLEAST ATTEMPT_ID""".split()


def ingest():
    """Ingest the data."""

    db.delete_dataset_records(DATASET_ID)

    to_taxon_id = get_taxa()
    raw_data = get_raw_data(to_taxon_id)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Nestwatch',
        'version': '2020-10-14',
        'url': ''})

    insert_places(raw_data)
    insert_events_and_counts(raw_data)


def get_taxa():
    """
    Get all taxa with a species_code.

    All of the Nestwatch taxa are already in the taxa table and they use the
    eBird_species_code_2018 as the taxon_id.
    """
    log(f'Selecting {DATASET_ID} taxa')

    sql = """
        SELECT taxon_id,
         JSON_EXTRACT(taxon_json, '$.eBird_species_code_2018') AS species_code
         FROM taxa
        WHERE class = 'aves'
          AND species_code IS NOT NULL
          """
    taxa = pd.read_sql(sql, db.connect())
    return taxa.set_index('species_code').taxon_id.to_dict()


def get_raw_data(to_taxon_id):
    """Read raw data."""
    log(f'Getting {DATASET_ID} raw data')

    raw_data = pd.read_csv(DATA_CSV, dtype='unicode').fillna('')

    raw_data['dataset_id'] = DATASET_ID
    raw_data['taxon_id'] = raw_data['SPECIES_CODE'].map(to_taxon_id)
    raw_data['lng'] = pd.to_numeric(raw_data['LONGITUDE'], errors='coerce')
    raw_data['lat'] = pd.to_numeric(raw_data['LATITUDE'], errors='coerce')

    raw_data['lay_date'] = raw_data['FIRST_LAY_DT'].str.split(':', 2).str[0]
    raw_data['lay_date'] = pd.to_datetime(
        raw_data['lay_date'], format='%d%b%Y', errors='coerce')

    raw_data['hatch_date'] = raw_data['HATCH_DT'].str.split(':', 2).str[0]
    raw_data['hatch_date'] = pd.to_datetime(
        raw_data['hatch_date'], format='%d%b%Y', errors='coerce')

    raw_data['fledge_date'] = raw_data['FLEDGE_DT'].str.split(':', 2).str[0]
    raw_data['fledge_date'] = pd.to_datetime(
        raw_data['fledge_date'], format='%d%b%Y', errors='coerce')

    has_taxon_id = raw_data['taxon_id'].notna()
    has_lng = raw_data['lng'].notna()
    has_lat = raw_data['lat'].notna()
    has_date = (raw_data['lay_date'].notna()
                | raw_data['hatch_date'].notna()
                | raw_data['fledge_date'].notna())
    keep = has_taxon_id & has_lng & has_lat & has_date
    raw_data = raw_data.loc[keep, :].copy()

    return raw_data


def insert_places(raw_data):
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    place_id = db.next_id('places')
    raw_data['place_id'] = pd.factorize(raw_data['LOC_ID'])[0] + place_id

    places = raw_data.drop_duplicates('LOC_ID').copy()
    places['radius'] = None

    fields = """LOC_ID SUBNATIONAL1_CODE ELEVATION_M HEIGHT_M
        REL_TO_SUBSTRATE SUBSTRATE_CODE""".split()
    places['place_json'] = util.json_object(places, fields)

    places.loc[:, db.PLACE_FIELDS].to_sql(
        'places', db.connect(), if_exists='append', index=False)


def insert_events_and_counts(raw_data):
    """Insert events and counts."""
    log(f'Inserting {DATASET_ID} events and counts')

    aggs = {x: pd.Series.max for x in NUMBERS + DATES}
    strs = {x: first_string for x
            in DATES + EVENT_FIELDS[:-1] + COUNT_FIELDS[:-1]}
    aggs = {**aggs, **strs}
    raw_data = raw_data.groupby('ATTEMPT_ID').agg(aggs)

    raw_data['started'] = None
    raw_data['ended'] = None

    df = add_event_records(raw_data, 'FIRST_LAY_DT', 'lay_date')
    add_count_records(df, 'CLUTCH_SIZE_HOST_ATLEAST')

    df = add_event_records(raw_data, 'HATCH_DT', 'hatch_date')
    add_count_records(df, 'EGGS_HOST_UNH_ATLEAST')
    add_count_records(df, 'YOUNG_HOST_TOTAL_ATLEAST')

    df = add_event_records(raw_data, 'FLEDGE_DT', 'fledge_date')
    add_count_records(df, 'YOUNG_HOST_FLEDGED_ATLEAST')
    add_count_records(df, 'YOUNG_HOST_DEAD_ATLEAST')


def add_event_records(df, event_type, event_date):
    """Add event records for the event type."""
    log(f'Adding {DATASET_ID} event records for {event_type}')
    this_year = datetime.now().year
    df = df.loc[df[event_date].notnull(), :].copy()
    df['event_id'] = db.create_ids(df, 'events')
    df['dataset_id'] = DATASET_ID
    df['year'] = df[event_date].dt.strftime('%Y').astype(int)
    df['year'] = df['year'].apply(lambda x: x - 100 if x > this_year else x)
    df['day'] = df[event_date].dt.strftime('%j').astype(int)
    df['event_type'] = event_type
    df['event_json'] = util.json_object(df, EVENT_FIELDS)
    df.loc[:, db.EVENT_FIELDS].to_sql(
        'events', db.connect(), if_exists='append', index=False)
    return df


def add_count_records(df, count_type):
    """Add count records for the count type."""
    log(f'Adding {DATASET_ID} count records for {count_type}')
    has_count = pd.to_numeric(df[count_type], errors='coerce').notna()
    df = df.loc[has_count, :].copy()
    df[count_type] = df[count_type].astype(int)
    df['count_id'] = db.create_ids(df, 'counts')
    df['dataset_id'] = DATASET_ID
    df['count'] = df[count_type]
    df['count_type'] = count_type
    df['count_json'] = util.json_object(df, COUNT_FIELDS)
    df.loc[df['count'].notna(), db.COUNT_FIELDS].to_sql(
        'counts', db.connect(), if_exists='append', index=False)


def first_string(group):
    """Return the first value in the group."""
    for item in group:
        if item:
            return item
    return ''
