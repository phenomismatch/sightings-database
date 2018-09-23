"""Ingest Monitoring Avian Productivity and Survivorship data."""

import os
from pathlib import Path
from datetime import date
import pandas as pd
from simpledbf import Dbf5
import lib.db as db
import lib.util as util
from lib.log import log


DATASET_ID = 'maps'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
LIST = 'LIST17'
BAND = '1117BAND'
EFFORT = '1117EF'
STATIONS = 'STATIONS'


def ingest():
    """Ingest the data."""
    cxn = db.connect()

    convert_dbf_to_csv(LIST)
    convert_dbf_to_csv(BAND)
    convert_dbf_to_csv(EFFORT)

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'MAPS: Monitoring Avian Productivity and Survivorship',
        'extracted': str(date.today()),
        'version': '2017.0',
        'url': 'https://www.birdpop.org/pages/maps.php'})

    insert_codes(cxn)
    to_place_id = insert_places(cxn)
    to_event_id = insert_events(cxn, to_place_id)
    insert_counts(cxn, to_event_id)


def convert_dbf_to_csv(file_name):
    """Convert DBF files to CSV files."""
    csv_file = RAW_DIR / f'{file_name}.csv'
    dbf_file = RAW_DIR / f'{file_name}.DBF'
    if not os.path.exists(csv_file):
        log(f'Converting {dbf_file} files to {csv_file}')
        dbf = Dbf5(dbf_file)
        df = dbf.to_dataframe()
        df.to_csv(csv_file, index=False)


def insert_places(cxn):
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    csv_file = RAW_DIR / f'{STATIONS}.csv'
    raw_places = pd.read_csv(csv_file, dtype='unicode')

    # Remove records with a bad STA code
    raw_places.STA = raw_places.STA.fillna('').astype(str)
    good_sta = raw_places.STA.str.len() > 3
    raw_places = raw_places.loc[good_sta, :]

    # Remove records with bad lng or lat values
    raw_places = util.filter_lng_lat(raw_places, 'DECLNG', 'DECLAT')

    places = pd.DataFrame()

    raw_places['place_id'] = db.get_ids(raw_places, 'places')
    places['place_id'] = raw_places['place_id']

    places['dataset_id'] = DATASET_ID

    places['lng'] = raw_places['DECLNG']

    places['lat'] = raw_places['DECLAT']

    places['radius'] = raw_places.PRECISION.map({
        '01S': 30.92,
        '10S': 30.92 * 10,
        'BLK': 111.32 * 1000 * 10,
        '01M': 111.32 * 1000,
        '05S': 30.92 * 5,
        '10M': 111.32 * 1000 * 10})

    places['geohash'] = None

    fields = """STATION LOC STA STA2 NAME LHOLD HOLDCERT O NEARTOWN
        COUNTY STATE US REGION BLOCK LATITUDE LONGITUDE PRECISION SOURCE DATUM
        DECLAT DECLNG NAD83 ELEV STRATUM BCR HABITAT REG PASSED""".split()
    places['place_json'] = util.json_object(raw_places, fields)

    places['dataset_id'] = DATASET_ID

    places.to_sql('places', cxn, if_exists='append', index=False)

    # Build dictionary to map events to place IDs
    return raw_places.set_index('STA').place_id.to_dict()


def insert_events(cxn, to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    csv_file = RAW_DIR / f'{EFFORT}.csv'
    raw_events = pd.read_csv(csv_file, dtype='unicode')

    # Raw events are STA, DATE, STATION groups with min & max times
    convert_to_time(raw_events, 'START')
    convert_to_time(raw_events, 'END')
    raw_events = raw_events.groupby(['STA', 'DATE', 'STATION'])
    raw_events = raw_events.agg({'START': min, 'END': max})
    raw_events.reset_index(inplace=True)

    events = pd.DataFrame()

    raw_events['event_id'] = db.get_ids(raw_events, 'events')
    events['event_id'] = raw_events['event_id']

    raw_events['place_id'] = raw_events.STA.map(to_place_id).fillna(-999)
    events['place_id'] = raw_events['place_id']
    events.place_id = events.place_id.astype(int)

    raw_events['raw_date'] = pd.to_datetime(raw_events.DATE)
    events['year'] = raw_events.raw_date.dt.strftime('%Y')
    events['day'] = raw_events.raw_date.dt.strftime('%j')

    events['started'] = raw_events['START']

    events['ended'] = raw_events['END']

    fields = 'STA DATE STATION'.split()
    events['event_json'] = util.json_object(raw_events, fields, DATASET_ID)

    events.loc[events.place_id >= 0, :].to_sql(
        'events', cxn, if_exists='append', index=False)

    return raw_events.loc[raw_events.place_id >= 0, :].set_index(
        ['STA', 'DATE'], verify_integrity=True).event_id.to_dict()


def convert_to_time(df, column):
    """Convert the time field from string HHm format to HH:MM format."""
    is_na = pd.to_numeric(df[column], errors='coerce')
    df[column] = df[column].fillna('0').astype(str)
    df[column] = df[column].str.pad(4, fillchar='0', side='right')
    df[column] = pd.to_datetime(df[column], format='%H%M', errors='coerce')
    df[column] = df[column].dt.strftime('%H:%M')
    df.loc[is_na, column] = ''


def insert_counts(cxn, to_event_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    to_taxon_id = get_raw_taxons(cxn)

    csv_file = RAW_DIR / f'{BAND}.csv'
    raw_counts = pd.read_csv(csv_file, dtype='unicode')

    counts = pd.DataFrame()
    counts['count_id'] = db.get_ids(raw_counts, 'counts')

    raw_counts['key'] = tuple(zip(raw_counts.STA, raw_counts.DATE))
    counts['event_id'] = raw_counts.key.map(to_event_id)

    counts['taxon_id'] = raw_counts.SPEC.map(to_taxon_id)

    counts['count'] = 1

    fields = """LOC BI BS PG C OBAND BAND SSN NUMB OSP SPEC OSP6 SPEC6 OA OHA
        AGE HA HA OWRP OWRP WRP OS OHS SEX HS SK CP BP F BM FM FW JP WNG WEIGHT
        STATUS DATE TIME STA STATION NET ANET DISP NOTE PPC SSC PPF SSF TT RR
        HD UPP UNP BPL NF FP SW COLOR SC CC BC MC WC JC OV1 V1 VM V94 V95 V96
        V97 OVYR VYR N B A""".split()
    counts['count_json'] = util.json_object(raw_counts, fields, DATASET_ID)

    counts = counts[counts.event_id.notna() & counts.taxon_id.notna()]

    counts.to_sql('counts', cxn, if_exists='append', index=False)


def get_raw_taxons(cxn):
    """Get MAPS taxon data."""
    raw = pd.read_csv(RAW_DIR / f'{LIST}.csv')

    raw = raw.loc[:, ['SCINAME', 'SPEC']]
    raw = raw.set_index('SCINAME')

    sql = """SELECT sci_name, taxon_id FROM taxons"""
    taxons = pd.read_sql(sql, cxn).set_index('sci_name')

    taxons = taxons.merge(raw, how='inner', left_index=True, right_index=True)

    return taxons.set_index('SPEC').taxon_id.to_dict()


def insert_codes(cxn):
    """Insert codes."""
    log(f'Inserting {DATASET_ID} codes')
    codes = pd.read_csv(RAW_DIR / 'maps_codes.csv')
    codes['dataset_id'] = DATASET_ID
    codes.to_sql('codes', cxn, if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
