"""Ingest nest watch data."""

from pathlib import Path
import numpy as np
import pandas as pd
import lib.db as db
import lib.util as util
from lib.util import log


DATASET_ID = 'nestwatch'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
DATA_CSV = RAW_DIR / 'Nestwatch_2018_1026.csv'

DATES = 'FIRST_LAY_DT HATCH_DT FLEDGE_DT'.split()
EVENT_FIELDS = """LOC_ID CAVITY_ENTRANCE_DIAM_CM
    ENTRANCE_ORIENTATION HABITAT_CODE_1 HABITAT_CODE_2 HABITAT_CODE_3
    PROJ_PERIOD_ID USER_ID OUTCOME_CODE_LIST place_id event_type""".split()
COUNT_FIELDS = "SPECIES_CODE taxon_id count_type""".split()
NUMBERS = """CLUTCH_SIZE_HOST_ATLEAST EGGS_HOST_UNH_ATLEAST
    YOUNG_HOST_TOTAL_ATLEAST YOUNG_HOST_FLEDGED_ATLEAST
    YOUNG_HOST_DEAD_ATLEAST""".split()


def ingest():
    """Ingest the data."""

    db.delete_dataset(DATASET_ID)

    to_taxon_id = get_taxa()
    raw_data = get_raw_data(to_taxon_id)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Nestwatch',
        'version': '2018-10-26',
        'url': ''})

    insert_places(raw_data)
    insert_events_and_counts(raw_data)


def get_taxa():
    """Get taxa."""
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
    raw_data = pd.read_csv(DATA_CSV, dtype='unicode')

    raw_data['dataset_id'] = DATASET_ID
    raw_data['taxon_id'] = raw_data['SPECIES_CODE'].map(to_taxon_id)
    raw_data['lng'] = pd.to_numeric(raw_data['LONGITUDE'], errors='coerce')
    raw_data['lat'] = pd.to_numeric(raw_data['LATITUDE'], errors='coerce')

    has_taxon_id = raw_data['taxon_id'].notna()
    has_lng = raw_data['lng'].notna()
    has_lat = raw_data['lat'].notna()
    raw_data = raw_data.loc[has_taxon_id & has_lng & has_lat, :].copy()

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

    aggs = {x: np.max for x in NUMBERS}
    strs = {x: first_string for x
            in DATES + EVENT_FIELDS[:-1] + COUNT_FIELDS[:-1]}
    aggs = {**aggs, **strs}
    raw_data = raw_data.groupby('ATTEMPT_ID').agg(aggs)

    raw_data['lay_date'] = pd.to_datetime(
        raw_data['FIRST_LAY_DT'], errors='coerce')
    raw_data['hatch_date'] = pd.to_datetime(
        raw_data['HATCH_DT'], errors='coerce')
    raw_data['fledge_date'] = pd.to_datetime(
        raw_data['FLEDGE_DT'], errors='coerce')
    raw_data['started'] = None
    raw_data['ended'] = None

    dfm = filter_records(raw_data, 'lay_date', 'CLUTCH_SIZE_HOST_ATLEAST')
    add_event_records(dfm, 'FIRST_LAY_DT', 'lay_date')
    add_count_records(dfm, 'CLUTCH_SIZE_HOST_ATLEAST')

    dfm = filter_records(
        raw_data, 'hatch_date',
        'EGGS_HOST_UNH_ATLEAST', 'YOUNG_HOST_TOTAL_ATLEAST')
    add_event_records(dfm, 'HATCH_DT', 'hatch_date')
    add_count_records(dfm, 'EGGS_HOST_UNH_ATLEAST')
    add_count_records(dfm, 'YOUNG_HOST_TOTAL_ATLEAST')

    dfm = filter_records(
        raw_data, 'hatch_date',
        'YOUNG_HOST_FLEDGED_ATLEAST', 'YOUNG_HOST_DEAD_ATLEAST')
    add_event_records(dfm, 'FLEDGE_DT', 'fledge_date')
    add_count_records(dfm, 'YOUNG_HOST_FLEDGED_ATLEAST')
    add_count_records(dfm, 'YOUNG_HOST_DEAD_ATLEAST')


def filter_records(dfm, event_date, count1, count2=None):
    """Filter records based upon the event date and count fields."""
    log(f'Filtering {DATASET_ID} event & counts records for {event_date}')
    has_date = dfm[event_date].notna()
    has_count1 = dfm[count1].notna()
    has_count2 = dfm[count2].notna() if count2 else has_count1
    return dfm.loc[has_date & (has_count1 | has_count2), :].copy()


def add_event_records(dfm, event_type, date_column):
    """Add event records for the event type."""
    log(f'Adding {DATASET_ID} event records for {event_type}')
    dfm['event_id'] = db.get_ids(dfm, 'events')
    dfm['dataset_id'] = DATASET_ID
    dfm['year'] = dfm[date_column].dt.strftime('%Y')
    dfm['day'] = dfm[date_column].dt.strftime('%j')
    dfm['event_type'] = event_type
    dfm['event_json'] = util.json_object(dfm, EVENT_FIELDS)
    dfm.loc[:, db.EVENT_FIELDS].to_sql(
        'events', db.connect(), if_exists='append', index=False)

def add_count_records(dfm, count_type):
    """Add count records for the count type."""
    log(f'Adding {DATASET_ID} count records for {count_type}')
    dfm['count_id'] = db.get_ids(dfm, 'counts')
    dfm['dataset_id'] = DATASET_ID
    dfm['count'] = dfm[count_type]
    dfm['count_type'] = count_type
    dfm['count_json'] = util.json_object(dfm, COUNT_FIELDS)
    dfm.loc[dfm['count'].notna(), db.COUNT_FIELDS].to_sql(
        'counts', db.connect(), if_exists='append', index=False)


def first_string(group):
    """Return the first value in the group."""
    for item in group:
        if item:
            return item
    return ''
