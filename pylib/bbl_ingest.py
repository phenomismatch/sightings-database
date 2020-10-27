"""Ingest USGS Bird Banding Laboratory data."""

from pathlib import Path

import pandas as pd

from . import db, util

DATASET_ID = 'bbl'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
BANDING = RAW_DIR / 'Banding'
ENCOUNTERS = RAW_DIR / 'Encounters'
RECAPTURES = RAW_DIR / 'Recaptures'

PLACE_FIELDS = """ """.split()
EVENT_FIELDS = """ """.split()
COUNT_FIELDS = """ """.split()

ALL_FIELDS = """
    B_AGE_CODE B_REGION B_BAND_NUM B_BAND_SIZE_CODE B_BAND_STATUS_CODE
    B_BAND_TYPE_CODE B_BIRD_STATUS B_COMMENTS B_COORD_PRECISION
    B_DIRECTION_CODE B_EXTRA_INFO_CODE B_FLYWAY_CODE B_HOW_AGED_CODE
    B_HOW_SEXED_CODE B_LAT_10_MIN_BLK B_LAT_DECIMAL_DEGREES B_LON_10_MIN_BLK
    B_LON_DECIMAL_DEGREES B_AUX_MARKER B_PERMIT_NUM B_REMARKS
    B_REWARD_BAND_NUM B_SEX_CODE B_SPECIES_ID BANDING_DATE BANDING_DAY
    BANDING_MONTH BANDING_YEAR B_MARKER_LONG_DESC B_SPECIES_NAME
    E_REGION E_CERTIFICATE_FLAG E_CERTIFICATE_LANGUAGE E_COMMENTS
    E_COORD_PRECISION E_COUNTRY_CODE E_COUNTY_CODE E_CREATE_DATE
    E_CREATE_MONTH E_CREATE_YEAR E_DIRECTION_CODE E_DIRECTION1 E_DIRECTION2
    E_ENC_COUNTY_ORIGINAL E_FLYWAY_CODE E_HOW_OBTAINED_CODE
    E_HOW_OBTAINED_DESC E_LAT_10_MIN_BLK E_LAT_DECIMAL_DEGREES
    E_LOCATION_DESCRIPTION E_LON_10_MIN_BLK E_LON_DECIMAL_DEGREES
    E_MARKER_CODE_COLOR_ID E_MARKER_COLOR_ID E_MARKER_DESC E_MARKER_REF_ID
    E_AUX_MARKER E_AUX_MARKER_CODE E_MILES1 E_MILES2 E_PLACE_NAME
    E_PRESENT_CONDITION_CODE E_REMARKS E_SPECIES_NAME E_STATE_CODE
    E_WHO_OBTAINED_CODE E_WHY_REPORTED_CODE ENCOUNTER_DATE ENCOUNTER_DAY
    ENCOUNTER_MONTH ENCOUNTER_YEAR E_MARKER_LONG_DESC HSS MIN_AGE_AT_ENC
    ORIGINAL_BAND BEARING DISTANCE SAME_10_MIN_BLOCK
    WAS_STATUS_CHANGED REC_SOURCE ENC_ERROR
    TYPE 10_MIN_BLOCK
    AGE_CODE REGION B_LOCATION_DESCRIPTION BAND_NUM BAND_SIZE_CODE
    BAND_STATUS_CODE BAND_TYPE_CODE BANDIT_LOCATION_ID BIRD_STATUS COMMENTS
    COORD_PRECISION COUNTRY_CODE DIRECTION_CODE ERRORS_COMMENTS_DESC
    EXTRA_INFO_CODE FLYWAY_CODE HOW_AGED_CODE HOW_SEXED_CODE LAT_10_MIN_BLK 
    LAT_DECIMAL_DEGREES LOCATION_ID LON_10_MIN_BLK LON_DECIMAL_DEGREES 
    OTHER_BANDS PERMIT_NUM SEX_CODE SPECIES_ID SPECIES_NAME STATE_CODE
    B_10_MIN_BLOCK B_COUNTRY_CODE B_HOW_CAPTURED B_LOCATION_ID B_STATE_CODE 
    E_10_MIN_BLOCK E_CAPTURE_TIME E_HOW_CAPTURED E_LOCATION_ID 
    E_REPORT_LOCATION_CHOICE ENC_PERMIT E_ERROR_CLEARED_DATE E_SPECIES_ID 
    REPORTING_METHOD """.split()


def ingest():
    """Ingest USGS Bird Banding Laboratory data."""
    db.delete_dataset_records(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Bird Banding Laboratory (BBL)',
        'version': '2020.0',
        'url': ('https://www.usgs.gov/centers/pwrc/science/'
                'bird-banding-laboratory')})

    create_raw_table()
    insert_raw_data('banding', BANDING)
    insert_raw_data('encounters', ENCOUNTERS)
    insert_raw_data('recaptures', RECAPTURES)

    insert_places()
    insert_events()
    insert_counts()


def create_raw_table():
    """Create the raw data table."""
    fields = [f"'{f}' TEXT" for f in ALL_FIELDS]
    fields = ','.join(fields)
    sql = f"""
        DROP TABLE IF EXISTS bbl_data;
        CREATE TABLE bbl_data ({fields});"""
    with db.connect() as cxn:
        cxn.executescript(sql)


def insert_raw_data(data_type, dir_):
    """Read in and clean up the raw data."""
    util.log(f'Inserting raw {DATASET_ID} {data_type} data')

    with db.connect() as cxn:
        for path in sorted(dir_.glob('*.csv')):
            util.log(f'File {path}')
            df = pd.read_csv(path, dtype='unicode').fillna('')
            util.normalize_columns_names(df)
            df['TYPE'] = data_type
            df.to_sql('bbl_data', cxn, if_exists='append', index=False)


def insert_places():
    """Insert BBL place data."""


def insert_events():
    """Insert BBL place data."""


def insert_counts():
    """Insert BBL place data."""


if __name__ == '__main__':
    ingest()
