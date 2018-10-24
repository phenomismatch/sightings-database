"""Ingest NABA data."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util
from lib.util import log


DATASET_ID = 'naba'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
DATA_CSV = RAW_DIR / 'NABA_JULY4_V2.csv'


def ingest():
    """Ingest the data."""
    raw_data = get_raw_data()

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'NABA',
        'version': '2018-07-04',
        'url': ''})

    to_taxon_id = insert_taxa(raw_data)
    to_place_id = insert_places(raw_data)
    to_event_id = insert_events(raw_data, to_place_id)
    insert_counts(raw_data, to_event_id, to_taxon_id)


def get_raw_data():
    """Read raw data."""
    log(f'Getting {DATASET_ID} raw data')

    raw_data = pd.read_csv(DATA_CSV, dtype='unicode')
    util.normalize_columns_names(raw_data)

    raw_data.LATITUDE = pd.to_numeric(raw_data.LATITUDE, errors='coerce')
    raw_data.LONGITUDE = pd.to_numeric(raw_data.LONGITUDE, errors='coerce')
    raw_data.iYear = raw_data.iYear.astype(float).astype(int)
    raw_data.Month = raw_data.Month.astype(float).astype(int)
    raw_data.Day = raw_data.Day.astype(float).astype(int)

    raw_data['SumOfBFLY_COUNT'] = raw_data[
        'SumOfBFLY_COUNT'].fillna(0).astype(float).astype(int)

    raw_data['sci_name'] = raw_data.apply(
        lambda x: f'{x["Gen_Tribe_Fam"]} {x.Species}', axis='columns')

    has_lng = raw_data.LONGITUDE.notna()
    has_lat = raw_data.LATITUDE.notna()
    has_year = raw_data.iYear.notna()
    has_month = raw_data.Month.notna()
    has_day = raw_data.Day.notna()
    raw_data = raw_data.loc[
        has_lng & has_lat & has_year & has_month & has_day].copy()

    return raw_data


def insert_taxa(raw_data):
    """Insert taxa."""
    log(f'Inserting {DATASET_ID} taxa')

    cxn = db.connect()

    firsts = raw_data.sci_name.duplicated(keep='first')
    taxa = raw_data.loc[firsts, ['sci_name']]

    taxa['genus'] = taxa.sci_name.str.split().str[0]
    taxa['dataset_id'] = DATASET_ID
    taxa['class'] = 'lepidoptera'
    taxa['group'] = None
    taxa['order'] = None
    taxa['family'] = None
    taxa['target'] = 't'
    taxa['common_name'] = ''

    taxa = db.drop_duplicate_taxa(taxa)
    taxa['taxon_id'] = db.get_ids(taxa, 'taxa')
    taxa.taxon_id = taxa.taxon_id.astype(int)

    taxa.to_sql('taxa', cxn, if_exists='append', index=False)

    sql = """SELECT sci_name, taxon_id
               FROM taxa
              WHERE "class" = 'lepidoptera'
                AND target = 't'"""
    return pd.read_sql(sql, cxn).set_index('sci_name').taxon_id.to_dict()


def insert_places(raw_data):
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    raw_places = raw_data.drop_duplicates(['LONGITUDE', 'LATITUDE']).copy()

    places = pd.DataFrame()

    raw_places['place_id'] = db.get_ids(raw_places, 'places')
    places['place_id'] = raw_places['place_id']

    places['dataset_id'] = DATASET_ID

    places['lng'] = pd.to_numeric(raw_places['LONGITUDE'], errors='coerce')

    places['lat'] = pd.to_numeric(raw_places['LATITUDE'], errors='coerce')

    places['radius'] = None

    places['radius'] = None
    places['dataset_id'] = DATASET_ID

    places['place_json'] = util.json_object(raw_places, ['SITE_ID'])

    places.to_sql('places', db.connect(), if_exists='append', index=False)

    return raw_places.reset_index().set_index(
        ['LONGITUDE', 'LATITUDE'], verify_integrity=True).place_id.to_dict()


def insert_events(raw_data, to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    raw_events = raw_data.drop_duplicates(['iYear', 'Month', 'Day']).copy()

    raw_events['date'] = raw_events.apply(
        lambda x: f'{x.iYear}-{x.Month}-{x.Day}', axis=1)
    raw_events['date'] = pd.to_datetime(raw_events['date'])

    events = pd.DataFrame()

    raw_events['event_id'] = db.get_ids(raw_events, 'events')
    events['event_id'] = raw_events.event_id

    raw_events['place_key'] = tuple(zip(
        raw_events.LONGITUDE, raw_events.LATITUDE))
    raw_events['place_id'] = raw_events.place_key.map(to_place_id)
    events['place_id'] = raw_events.place_id

    events['year'] = raw_events['date'].dt.strftime('%Y')
    events['day'] = raw_events['date'].dt.strftime('%j')

    events['started'] = None
    events['ended'] = None

    fields = 'iYear Month Day PARTY_HOURS'.split()
    events['event_json'] = util.json_object(raw_events, fields, DATASET_ID)

    events.to_sql('events', db.connect(), if_exists='append', index=False)

    return raw_events.reset_index().set_index(
        ['iYear', 'Month', 'Day'], verify_integrity=True).event_id.to_dict()


def insert_counts(raw_data, to_event_id, to_taxon_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    raw_data['key'] = tuple(zip(raw_data.iYear, raw_data.Month, raw_data.Day))

    counts = pd.DataFrame()

    counts['count_id'] = db.get_ids(raw_data, 'counts')

    counts['event_id'] = raw_data.key.map(to_event_id)

    counts['taxon_id'] = raw_data.sci_name.map(to_taxon_id)

    counts['count'] = raw_data.SumOfBFLY_COUNT

    fields = 'SPECIES_CODE Gen_Tribe_Fam Species'.split()
    counts['count_json'] = util.json_object(raw_data, fields, DATASET_ID)

    has_event_id = counts.event_id.notna()
    has_taxon_id = counts.taxon_id.notna()
    has_count = counts['count'].notna()
    counts = counts.loc[has_event_id & has_taxon_id & has_count, :]

    counts.to_sql('counts', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
