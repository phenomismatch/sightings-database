"""Ingest eBird data."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util
from lib.util import log


DATASET_ID = 'ebird'
RAW_DIR = Path('data') / 'raw' / DATASET_ID


def ingest():
    """Ingest eBird data."""
    db.delete_dataset(DATASET_ID)

    to_taxon_id = get_taxa()

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'eBird Basic Dataset',
        'version': 'relFeb-2018',
        'url': 'https://ebird.org/home'})

    chunk = 1_000_000
    reader = pd.read_csv(
        RAW_DIR / 'ebd_relFeb-2018.txt',
        delimiter='\t',
        quoting=3,
        chunksize=chunk,
        dtype='unicode')

    to_place_id = {}
    to_event_id = {}

    for i, raw_data in enumerate(reader, 1):
        log(f'Processing {DATASET_ID} chunk {i * chunk:,}')

        raw_data = filter_data(raw_data, to_taxon_id)

        if raw_data.shape[0] == 0:
            continue

        to_place_id = insert_places(raw_data, to_place_id)
        to_event_id = insert_events(raw_data, to_place_id, to_event_id)
        insert_counts(raw_data, to_event_id, to_taxon_id)


def get_taxa():
    """Build a dictionary of scientific names and taxon_ids."""
    sql = """SELECT taxon_id, sci_name
               FROM taxa
              WHERE target = 't'
                AND "class"='aves'"""
    taxa = pd.read_sql(sql, db.connect())
    return taxa.set_index('sci_name').taxon_id.to_dict()


def filter_data(raw_data, to_taxon_id):
    """Limit the size & scope of the data."""
    raw_data = raw_data.rename(columns={
        'LONGITUDE': 'lng',
        'LATITUDE': 'lat',
        'EFFORT DISTANCE KM': 'radius',
        'TIME OBSERVATIONS STARTED': 'started',
        ' LOCALITY TYPE': 'LOCALITY TYPE',
        'OBSERVATION COUNT': 'count'})
    util.normalize_columns_names(raw_data)

    raw_data['date'] = pd.to_datetime(
        raw_data['OBSERVATION_DATE'], errors='coerce')
    raw_data['count'] = pd.to_numeric(raw_data['count'], errors='coerce')

    has_date = raw_data.date.notna()
    has_count = raw_data['count'].notna()
    is_approved = raw_data.APPROVED == '1'
    is_complete = raw_data['ALL_SPECIES_REPORTED'] == '1'
    in_species = raw_data['SCIENTIFIC_NAME'].isin(to_taxon_id)

    raw_data = raw_data[
        has_date & has_count & is_approved & is_complete & in_species]

    return util.filter_lng_lat(
        raw_data, 'lng', 'lat', lng=(-95.0, -50.0), lat=(20.0, 90.0))


def insert_places(raw_data, to_place_id):
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    raw_data['place_key'] = tuple(zip(raw_data.lng, raw_data.lat))

    places = raw_data.drop_duplicates('place_key')

    old_places = places.place_key.isin(to_place_id)
    places = places[~old_places]

    places['place_id'] = db.get_ids(places, 'places')

    places['dataset_id'] = DATASET_ID

    is_na = places.radius.isna()
    places.radius = pd.to_numeric(
        places.radius, errors='coerce').fillna(0.0)
    places.radius *= 1000.0
    places.loc[is_na, 'radius'] = None

    fields = """COUNTRY_CODE STATE_CODE COUNTY_CODE IBA_CODE BCR_CODE
        USFWS_CODE ATLAS_BLOCK LOCALITY_ID LOCALITY_TYPE
        EFFORT_AREA_HA""".split()
    places['place_json'] = util.json_object(places, fields)

    places.loc[:, db.PLACE_FIELDS].to_sql(
        'places', db.connect(), if_exists='append', index=False)

    places['place_key'] = tuple(zip(places.lng, places.lat))
    new_place_ids = places.set_index('place_key').place_id.to_dict()
    return {**to_place_id, **new_place_ids}


def insert_events(raw_data, to_place_id, to_event_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    events = raw_data.drop_duplicates('SAMPLING_EVENT_IDENTIFIER')

    old_events = events.SAMPLING_EVENT_IDENTIFIER.isin(to_event_id)
    events = events[~old_events]

    events['event_id'] = db.get_ids(events, 'events')

    events['place_key'] = tuple(zip(events.lng, events.lat))
    events['place_id'] = events.place_key.map(to_place_id)

    events['year'] = events.date.dt.strftime('%Y')
    events['day'] = events.date.dt.strftime('%j')

    events['started'] = pd.to_datetime(events['started'], format='%H:%M:%S')
    events['delta'] = pd.to_numeric(events.DURATION_MINUTES, errors='coerce')
    events.delta = pd.to_timedelta(events.delta, unit='m', errors='coerce')
    events['ended'] = events.started + events.delta
    convert_to_time(events, 'started')
    convert_to_time(events, 'ended')

    fields = """SAMPLING_EVENT_IDENTIFIER EFFORT_AREA_HA APPROVED REVIEWED
        NUMBER_OBSERVERS ALL_SPECIES_REPORTED OBSERVATION_DATE GROUP_IDENTIFIER
        DURATION_MINUTES""".split()
    events['event_json'] = util.json_object(events, fields, DATASET_ID)

    events.loc[:, db.EVENT_FIELDS].to_sql(
        'events', db.connect(), if_exists='append', index=False)

    new_event_ids = events.set_index(
        'SAMPLING_EVENT_IDENTIFIER').event_id.to_dict()
    return {**to_event_id, **new_event_ids}


def convert_to_time(df, column):
    """Convert the time field from datetime format to HH:MM format."""
    is_na = df[column].isna()
    df[column] = df[column].dt.strftime('%H:%M')
    df.loc[is_na, column] = None


def insert_counts(counts, to_event_id, to_taxon_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    counts['count_id'] = db.get_ids(counts, 'counts')

    counts['event_id'] = counts.SAMPLING_EVENT_IDENTIFIER.map(to_event_id)

    counts['taxon_id'] = counts.SCIENTIFIC_NAME.map(to_taxon_id)

    fields = """SCIENTIFIC_NAME GLOBAL_UNIQUE_IDENTIFIER LAST_EDITED_DATE
        TAXONOMIC_ORDER CATEGORY SUBSPECIES_SCIENTIFIC_NAME
        BREEDING_BIRD_ATLAS_CODE BREEDING_BIRD_ATLAS_CATEGORY AGE_SEX
        OBSERVER_ID HAS_MEDIA""".split()
    counts['count_json'] = util.json_object(counts, fields, DATASET_ID)

    counts.loc[:, db.COUNT_FIELDS].to_sql(
        'counts', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
