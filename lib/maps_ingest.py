"""
Ingest Monitoring Avian Productivity and Survivorship (MAPS) data.

This is a bird banding survey, so there is going to be a count of one for each
observation. The data has is broken into 4 files:
1) A list of species.
2) A list of banding stations, which correspond to the places table.
3) An effort table containing information about the banding conditions. This
    corresponds with the events table.
4) The band table which contains information about the bird's condition. This
    corresponds with the counts table. A count of one is assumed.
"""

import os
from pathlib import Path
import pandas as pd
from simpledbf import Dbf5
import lib.db as db
import lib.util as util
from lib.util import log


DATASET_ID = 'maps'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
LIST = 'LIST18'
BAND = '1106B18'
EFFORT = '1106E18'
STATIONS = 'STATIONS'


def ingest():
    """Ingest the data."""
    convert_dbf_to_csv(LIST)
    convert_dbf_to_csv(BAND)
    convert_dbf_to_csv(EFFORT)

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'MAPS: Monitoring Avian Productivity and Survivorship',
        'version': '2017.0',
        'url': 'https://www.birdpop.org/pages/maps.php'})

    to_taxon_id = insert_taxa()
    to_place_id = insert_places()
    to_event_id = insert_events(to_place_id)
    insert_counts(to_event_id, to_taxon_id)


def convert_dbf_to_csv(file_name):
    """Convert DBF files to CSV files."""
    csv_file = RAW_DIR / f'{file_name}.csv'
    dbf_file = RAW_DIR / f'{file_name}.DBF'
    if not os.path.exists(csv_file):
        log(f'Converting {dbf_file} files to {csv_file}')
        dbf = Dbf5(dbf_file)
        dfm = dbf.to_dataframe()
        dfm.to_csv(csv_file, index=False)


def insert_taxa():
    """
    Get MAPS taxon data.

    Using the SPEC field is used as the taxon ID in this data.
    """
    log(f'Inserting {DATASET_ID} taxa')

    raw_taxa = pd.read_csv(RAW_DIR / f'{LIST}.csv').fillna('')
    raw_taxa.rename(
        columns={'SCINAME': 'sci_name', 'COMMONNAME': 'common_name'},
        inplace=True)
    raw_taxa.sci_name = raw_taxa.sci_name.str.split().str.join(' ')

    taxa = raw_taxa.copy()
    taxa = db.drop_duplicate_taxa(taxa)
    taxa['taxon_id'] = db.get_ids(taxa, 'taxa')
    taxa.taxon_id = taxa.taxon_id.astype(int)
    taxa['group'] = None
    taxa['class'] = 'aves'
    taxa['order'] = None
    taxa['family'] = None
    taxa['genus'] = taxa.sci_name.str.split().str[0]
    taxa['category'] = None
    taxa['target'] = None
    fields = 'SP SPEC CONF SPEC6 CONF6'.split()
    taxa['taxon_json'] = util.json_object(taxa, fields)
    taxa.loc[:, db.TAXON_FIELDS].to_sql(
        'taxa', db.connect(), if_exists='append', index=False)

    raw_taxa = raw_taxa.set_index('sci_name')
    sql = """SELECT * FROM taxa"""
    taxa = pd.read_sql(sql, db.connect()).set_index('sci_name')
    taxa = taxa.merge(raw_taxa, how='inner', left_index=True, right_index=True)
    db.update_taxa_json(taxa, fields)

    to_taxon_id = taxa.set_index('SPEC').taxon_id.to_dict()
    return to_taxon_id


def insert_places():
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

    fields = """STATION LOC STA STA2 NAME LHOLD HOLDCERT O NEARTOWN
        COUNTY STATE US REGION BLOCK LATITUDE LONGITUDE PRECISION SOURCE DATUM
        DECLAT DECLNG NAD83 ELEV STRATUM BCR HABITAT REG PASSED""".split()
    places['place_json'] = util.json_object(raw_places, fields)
    places['dataset_id'] = DATASET_ID
    places.to_sql('places', db.connect(), if_exists='append', index=False)

    # Build dictionary to map events to place IDs
    return raw_places.set_index('STA').place_id.to_dict()


def insert_events(to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    csv_file = RAW_DIR / f'{EFFORT}.csv'
    raw_events = pd.read_csv(csv_file, dtype='unicode')

    # Raw events are STA, DATE, STATION groups with min & max times
    convert_to_time(raw_events, 'START')
    convert_to_time(raw_events, 'END')
    raw_events['LENGTH_MAX'] = raw_events['LENGTH']
    raw_events = raw_events.groupby(['STA', 'STATION', 'DATE', 'NET'])
    raw_events = raw_events.agg({'START': min, 'END': max, 'LENGTH': max})
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
    events['dataset_id'] = DATASET_ID
    fields = 'STA DATE STATION NET LENGTH'.split()
    events['event_json'] = util.json_object(raw_events, fields)
    events.loc[events.place_id >= 0, :].to_sql(
        'events', db.connect(), if_exists='append', index=False)

    return raw_events.loc[raw_events.place_id >= 0, :].set_index(
        ['STA', 'DATE', 'NET'], verify_integrity=True).event_id.to_dict()


def convert_to_time(dfm, column):
    """Convert the time field from string HHm format to HH:MM format."""
    is_na = pd.to_numeric(dfm[column], errors='coerce')
    dfm[column] = dfm[column].fillna('0').astype(str)
    dfm[column] = dfm[column].str.pad(4, fillchar='0', side='right')
    dfm[column] = pd.to_datetime(dfm[column], format='%H%M', errors='coerce')
    dfm[column] = dfm[column].dt.strftime('%H:%M')
    dfm.loc[is_na, column] = ''


def insert_counts(to_event_id, to_taxon_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    cxn = db.connect()
    csv_file = RAW_DIR / f'{BAND}.csv'
    raw_counts = pd.read_csv(csv_file, dtype='unicode')
    counts = pd.DataFrame()
    counts['count_id'] = db.get_ids(raw_counts, 'counts')
    raw_counts['key'] = tuple(zip(
        raw_counts.STA, raw_counts.DATE, raw_counts.NET))
    counts['event_id'] = raw_counts.key.map(to_event_id)
    counts['taxon_id'] = raw_counts.SPEC.map(to_taxon_id)
    counts['count'] = 1
    counts['dataset_id'] = DATASET_ID

    fields = """LOC BI BS PG C OBAND BAND SSN NUMB OSP SPEC OSP6 SPEC6 OA OHA
        AGE HA HA OWRP OWRP WRP OS OHS SEX HS SK CP BP F BM FM FW JP WNG WEIGHT
        STATUS DATE TIME STA STATION NET ANET DISP NOTE PPC SSC PPF SSF TT RR
        HD UPP UNP BPL NF FP SW COLOR SC CC BC MC WC JC OV1 V1 VM V94 V95 V96
        V97 OVYR VYR N B A""".split()
    counts['count_json'] = util.json_object(raw_counts, fields)
    counts = counts[counts.event_id.notna() & counts.taxon_id.notna()]
    counts.to_sql('counts', cxn, if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
