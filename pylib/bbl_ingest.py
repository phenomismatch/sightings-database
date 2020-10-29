"""Ingest USGS Bird Banding Laboratory data."""

from pathlib import Path

import pandas as pd

from . import db, util

DATASET_ID = 'bbl'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
BANDING = RAW_DIR / 'Banding'
ENCOUNTERS = RAW_DIR / 'Encounters'
RECAPTURES = RAW_DIR / 'Recaptures'
SPECIES = RAW_DIR / 'species.html'

ONE_MIN = 111.32 * 1000
TEN_MIN = 111.32 * 1000 * 10
EXACT = 0


def ingest():
    """Ingest USGS Bird Banding Laboratory data."""
    db.delete_dataset_records(DATASET_ID)

    to_taxon_id = get_taxa()

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Bird Banding Laboratory (BBL)',
        'version': '2020.0',
        'url': ('https://www.usgs.gov/centers/pwrc/science/'
                'bird-banding-laboratory')})

    to_place_id = {}

    to_place_id = insert_banding_data(to_place_id, to_taxon_id)
    to_place_id = insert_encounter_data(
        ENCOUNTERS, to_place_id, to_taxon_id, 'encounter')
    insert_encounter_data(RECAPTURES, to_place_id, to_taxon_id, 'recapture')


def get_taxa():
    """Build a taxa table to link to our taxa."""
    codes = pd.read_html(str(SPECIES))[0]
    codes = codes.rename(columns={
        'Scientific Name': 'sci_name',
        'Species Number': 'species_id'})

    codes = codes[codes['sci_name'].notna()]
    codes = codes.set_index('sci_name')['species_id'].to_dict()

    sql = """SELECT taxon_id, sci_name FROM taxa WHERE "class"='aves';"""
    taxa = pd.read_sql(sql, db.connect())
    taxa = taxa.set_index('sci_name')['taxon_id'].to_dict()

    to_taxon_id = {str(v).zfill(4): i for k, v in codes.items()
                   if (i := taxa.get(k))}

    return to_taxon_id


def insert_banding_data(to_place_id, to_taxon_id):
    """Insert raw banding data."""
    util.log(f'Inserting {DATASET_ID} banding data')

    for path in sorted(BANDING.glob('*.csv')):
        util.log(f'File {path}')

        df = read_csv(
            path, 'LON_DECIMAL_DEGREES', 'LAT_DECIMAL_DEGREES', 'banding')

        df = filter_data(
            df, to_taxon_id, 'BANDING_DATE', 'SPECIES_ID', 'COORD_PRECISION')

        to_place_id = insert_places(df, to_place_id, 'COORD_PRECISION')

        event_json = """ BAND_NUM BANDING_DATE TYPE """.split()
        insert_events(df, event_json)

        count_json = """
            AGE_CODE SEX_CODE SPECIES_ID SPECIES_NAME TYPE """.split()
        insert_counts(df, count_json)

    return to_place_id


def insert_encounter_data(dir_, to_place_id, to_taxon_id, type_):
    """Insert raw encounter and recapture data."""
    util.log(f'Inserting {DATASET_ID} {type_} data')

    for path in sorted(dir_.glob('*.csv')):
        util.log(f'File {path}')

        df = read_csv(
            path, 'E_LON_DECIMAL_DEGREES', 'E_LAT_DECIMAL_DEGREES', type_)

        df = filter_data(
            df, to_taxon_id,
            'ENCOUNTER_DATE', 'B_SPECIES_ID', 'E_COORD_PRECISION')

        to_place_id = insert_places(df, to_place_id, 'E_COORD_PRECISION')

        event_json = """ BAND_NUM ENCOUNTER_DATE TYPE """.split()
        insert_events(df, event_json)

        count_json = """
            B_AGE_CODE B_SEX_CODE B_SPECIES_ID B_SPECIES_NAME MIN_AGE_AT_ENC
            ORIGINAL_BAND TYPE """.split()
        insert_counts(df, count_json)

    return to_place_id


def read_csv(path, lng, lat, type_):
    """Read in a CSV file."""
    df = pd.read_csv(path, dtype='unicode').fillna('')
    util.normalize_columns_names(df)
    df = df.rename(columns={lng: 'lng', lat: 'lat'})
    df['TYPE'] = type_
    df['dataset_id'] = DATASET_ID
    return df


def filter_data(df, to_taxon_id, event_date, species_id, coord_precision):
    """Remove records that will not work for our analysis."""
    df['date'] = pd.to_datetime(df[event_date], errors='coerce')
    has_date = df['date'].notna()

    # Check if the scientific name is in our database
    df['taxon_id'] = df[species_id].map(to_taxon_id)
    has_taxon_id = df['taxon_id'].notna()

    # Country and state are too big of an area
    too_big = df[coord_precision].isin(['12', '72'])

    df = df.loc[~too_big & has_taxon_id & has_date]

    return df


def insert_places(df, to_place_id, coord_precision):
    """Insert place records."""
    util.filter_lng_lat(df, 'lng', 'lat')

    df['radius'] = TEN_MIN
    df.loc[df[coord_precision] == '0', 'radius'] = EXACT
    df.loc[df[coord_precision].isin(['1', '60']), 'radius'] = ONE_MIN

    df['place_key'] = tuple(zip(df.lng, df.lat, df.radius))

    places = df.drop_duplicates('place_key')

    old_places = places['place_key'].isin(to_place_id)
    places = places[~old_places]

    places['place_id'] = db.create_ids(places, 'places')

    places['place_json'] = util.json_object(places, [coord_precision])

    places.loc[:, db.PLACE_FIELDS].to_sql(
        'places', db.connect(), if_exists='append', index=False)

    new_place_ids = places.set_index('place_key')['place_id'].to_dict()
    to_place_id = {**to_place_id, **new_place_ids}

    df['place_id'] = df['place_key'].map(to_place_id)

    return to_place_id


def insert_events(df, event_json):
    """Insert event records."""
    df['event_id'] = db.create_ids(df, 'events')
    df['year'] = df['date'].dt.strftime('%Y')
    df['day'] = df['date'].dt.strftime('%j')
    df['started'] = None
    df['ended'] = None

    df['event_json'] = util.json_object(df, event_json)

    df.loc[:, db.EVENT_FIELDS].to_sql(
        'events', db.connect(), if_exists='append', index=False)


def insert_counts(df, count_json):
    """Insert count records."""
    df['count_id'] = db.create_ids(df, 'counts')
    df['count'] = 1

    df['count_json'] = util.json_object(df, count_json)

    df.loc[:, db.COUNT_FIELDS].to_sql(
        'counts', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
