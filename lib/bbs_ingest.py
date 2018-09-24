"""Ingest Breed Bird Survey data."""

from pathlib import Path
from datetime import date
import pandas as pd
import lib.db as db
import lib.util as util
from lib.log import log


DATASET_ID = 'bbs'
RAW_DIR = Path('data') / 'raw' / DATASET_ID


def ingest():
    """Ingest Breed Bird Survey data."""
    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'North American Breeding Bird Survey (BBS)',
        'extracted': str(date.today()),
        'version': '2016.0',
        'url': 'https://www.pwrc.usgs.gov/bbs/'})

    insert_codes()
    to_place_id = insert_places()
    to_event_id = insert_events(to_place_id)
    insert_counts(to_event_id)


def insert_places():
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    csv_path = RAW_DIR / 'bbs_places.csv'
    raw_places = pd.read_csv(csv_path, encoding='ISO-8859-1')

    places = pd.DataFrame()

    raw_places['place_id'] = db.get_ids(raw_places, 'places')
    places['place_id'] = raw_places['place_id']

    places['dataset_id'] = DATASET_ID

    places['lng'] = raw_places['longitude']

    places['lat'] = raw_places['latitude']

    places['radius'] = 1609.344 * 25  # twenty-five miles in meters

    places['geohash'] = None

    fields = """countrynum statenum route routename active stratum bcr
        landtypeid routetypeid routetypedetailid""".split()
    places['place_json'] = util.json_object(raw_places, fields)

    places.to_sql('places', db.connect(), if_exists='append', index=False)

    # Build dictionary to map events to place IDs
    return raw_places.set_index(['statenum', 'route']).place_id.to_dict()


def insert_events(to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    csv_path = RAW_DIR / 'bbs_events.csv'
    raw_events = pd.read_csv(csv_path, encoding='ISO-8859-1')

    events = pd.DataFrame()

    raw_events['event_id'] = db.get_ids(raw_events, 'events')
    events['event_id'] = raw_events['event_id']

    raw_events['place_key'] = tuple(zip(raw_events.statenum, raw_events.route))
    events['place_id'] = raw_events.place_key.map(to_place_id)

    events['year'] = raw_events['year']

    events['day'] = pd.to_datetime(
        raw_events.loc[:, ['year', 'month', 'day']]).dt.strftime('%j')

    events['started'] = raw_events['starttime']
    convert_to_time(events, 'started')

    events['ended'] = raw_events['endtime']
    convert_to_time(events, 'ended')

    fields = """routedataid countrynum statenum route rpid month day obsn
        totalspp starttemp endtemp tempscale startwind endwind startsky endsky
        assistant runtype""".split()
    events['event_json'] = util.json_object(raw_events, fields, DATASET_ID)

    events.to_sql('events', db.connect(), if_exists='append', index=False)

    # Build dictionary to map events to place IDs
    return raw_events.set_index(
        ['statenum', 'route', 'rpid', 'year']).event_id.to_dict()


def convert_to_time(df, column):
    """Convert the time field from int hMM format to HH:MM format."""
    is_na = pd.to_numeric(df[column], errors='coerce').isna()
    df[column] = df[column].fillna(0).astype(int).astype(str)
    df[column] = df[column].str.pad(4, fillchar='0')
    df[column] = pd.to_datetime(
        df[column], format='%H%M', errors='coerce').dt.strftime('%H:%M')
    df.loc[is_na, column] = ''


def insert_counts(to_event_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    cxn = db.connect()

    to_taxon_id = get_raw_taxons(cxn)

    csv_path = RAW_DIR / 'bbs_counts.csv'
    raw_counts = pd.read_csv(csv_path, encoding='ISO-8859-1')

    # Remove records that don't map to a valid taxon
    raw_counts['taxon_id'] = raw_counts.aou.map(to_taxon_id)
    has_taxon_id = raw_counts.taxon_id.notna()
    raw_counts = raw_counts.loc[has_taxon_id, :]

    counts = pd.DataFrame()

    counts['count_id'] = db.get_ids(raw_counts, 'counts')

    raw_counts['event_key'] = tuple(zip(
        raw_counts.statenum,
        raw_counts.route,
        raw_counts.rpid,
        raw_counts.year))
    counts['event_id'] = raw_counts.event_key.map(to_event_id)

    counts['taxon_id'] = raw_counts['taxon_id']

    counts['count'] = raw_counts['speciestotal'].fillna(0)

    fields = """record_id countrynum statenum route rpid year aou count10
        count20 count30 count40 count50 stoptotal""".split()
    counts['count_json'] = util.json_object(raw_counts, fields, DATASET_ID)

    counts = counts[counts.event_id.notna() & counts.taxon_id.notna()]

    counts.to_sql('counts', cxn, if_exists='append', index=False)


def get_raw_taxons():
    """Get BBS taxon data."""
    csv_path = RAW_DIR / 'bbs_taxons.csv'
    raw = pd.read_csv(csv_path, encoding='ISO-8859-1')

    raw['genus1'] = raw.genus.str.split().str[0]
    raw['species1'] = raw.species.str.split().str[0]
    raw['sci_name'] = raw.genus1 + ' ' + raw.species1
    raw = raw.loc[:, ['sci_name', 'aou']]
    raw = raw.set_index('sci_name')

    sql = """SELECT sci_name, taxon_id FROM taxons"""
    taxons = pd.read_sql(sql, db.connect()).set_index('sci_name')
    taxons = taxons.merge(raw, how='inner', left_index=True, right_index=True)

    return taxons.set_index('aou').taxon_id.to_dict()


def insert_codes():
    """Insert codes."""
    log(f'Inserting {DATASET_ID} codes')

    bcr = pd.read_fwf(
        RAW_DIR / 'BCR.txt',
        skiprows=7,
        encoding='ISO-8859-1',
        usecols=[0, 1],
        keep_default_na=False,
        names=['code', 'value'])
    bcr['field'] = 'bcr'

    strata = pd.read_fwf(
        RAW_DIR / 'BBSStrata.txt',
        skiprows=16,
        encoding='ISO-8859-1',
        usecols=[0, 1],
        keep_default_na=False,
        names=['code', 'value'])
    strata['field'] = 'strata'

    protocols = pd.read_fwf(
        RAW_DIR / 'RunProtocolID.txt',
        skiprows=4,
        encoding='ISO-8859-1',
        colspecs=[(0, 3), (5, 55)],
        names=['code', 'value'])
    protocols['field'] = 'runprotocol'

    descrs = pd.read_fwf(
        RAW_DIR / 'RunProtocolID.txt',
        skiprows=4,
        encoding='ISO-8859-1',
        colspecs=[(0, 3), (56, 141)],
        names=['code', 'value'])
    descrs['field'] = 'runprotocoldesc'

    wind = pd.read_fwf(
        RAW_DIR / 'weathercodes.txt',
        skiprows=8,
        skipfooter=13,
        encoding='ISO-8859-1',
        colspecs=[(0, 1), (7, 72)],
        keep_default_na=False,
        names=['code', 'value'])
    wind['field'] = 'wind'

    sky = pd.read_fwf(
        RAW_DIR / 'weathercodes.txt',
        skiprows=23,
        encoding='ISO-8859-1',
        colspecs=[(0, 1), (6, 56)],
        keep_default_na=False,
        names=['code', 'value'])
    sky['field'] = 'sky'

    states = pd.read_fwf(
        RAW_DIR / 'RegionCodes.txt',
        skiprows=11,
        usecols=[1, 2],
        encoding='ISO-8859-1',
        keep_default_na=False,
        names=['code', 'value'])
    states['field'] = 'state'

    types = pd.read_fwf(
        RAW_DIR / 'RouteInf.txt',
        skiprows=28,
        skipfooter=13,
        colspecs=[(3, 4), (7, 18)],
        encoding='ISO-8859-1',
        keep_default_na=False,
        names=['code', 'value'])
    types['field'] = 'routetype'

    details = pd.read_fwf(
        RAW_DIR / 'RouteInf.txt',
        skiprows=33,
        skipfooter=5,
        colspecs=[(3, 4), (7, 45)],
        encoding='ISO-8859-1',
        keep_default_na=False,
        names=['code', 'value'])
    details['field'] = 'routetypedetail'

    codes = bcr.append(
        [strata, protocols, descrs, wind, sky, states, types, details],
        ignore_index=True)
    codes['dataset_id'] = DATASET_ID
    codes.to_sql('codes', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
