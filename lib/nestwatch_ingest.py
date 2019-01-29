"""Ingest nest watch data."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util
from lib.util import log


DATASET_ID = 'nestwatch'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
DATA_CSV = RAW_DIR / 'Nestwatch_2018_1026.csv'


def ingest():
    """Ingest the data."""
    raw_data = get_raw_data()

    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Nestwatch',
        'version': '2018-10-26',
        'url': ''})

    to_taxon_id = get_taxa()
    to_place_id = insert_places(raw_data)
    to_event_id = insert_events(raw_data, to_place_id)
    insert_counts(raw_data, to_taxon_id, to_event_id)


def get_raw_data():
    """Read raw data."""
    log(f'Getting {DATASET_ID} raw data')
    raw_data = pd.read_csv(DATA_CSV, dtype='unicode')
    return raw_data


def get_taxa():
    """Get taxa."""
    log(f'Selecting {DATASET_ID} taxa')
    sql = """
        SELECT taxon_id,
         JSON_EXTRACT(taxon_json, '$.eBird_species_code_2018') AS species_code
         FROM taxa
        WHERE species_code IS NOT NULL"""
    taxa = pd.read_sql(sql, db.connect())
    return taxa.set_index('species_code').taxon_id.to_dict()


def insert_places(raw_data):
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    raw_places = raw_data.drop_duplicates('LOC_ID').copy()
    places = pd.DataFrame()

    raw_places['place_id'] = db.get_ids(raw_places, 'places')
    places['place_id'] = raw_places['place_id']

    places['lng'] = raw_places['LONGITUDE']
    places['lat'] = raw_places['LATITUDE']
    places['radius'] = None
    places['dataset_id'] = DATASET_ID

    fields = """LOC_ID SUBNATIONAL1_CODE ELEVATION_M HEIGHT_M
        REL_TO_SUBSTRATE SUBSTRATE_CODE""".split()
    places['place_json'] = util.json_object(raw_places, fields)

    places.to_sql('places', db.connect(), if_exists='append', index=False)

    return raw_places.reset_index().set_index(
        'LOC_ID', verify_integrity=True).place_id.to_dict()


def insert_events(raw_data, to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')
    print(raw_data.count())
    # ['LOC_ID', 'LATITUDE', 'LONGITUDE', 'SUBNATIONAL1_CODE', 'ELEVATION_M',
    #    'HEIGHT_M', 'REL_TO_SUBSTRATE', 'SUBSTRATE_CODE',
    #    'CAVITY_ENTRANCE_DIAM_CM', 'ENTRANCE_ORIENTATION', 'HABITAT_CODE_1',
    #    'HABITAT_CODE_2', 'HABITAT_CODE_3', 'ATTEMPT_ID', 'PROJ_PERIOD_ID',
    #    'USER_ID', 'SPECIES_CODE', 'OUTCOME_CODE_LIST', 'FIRST_LAY_DT',
    #    'HATCH_DT', 'FLEDGE_DT', 'CLUTCH_SIZE_HOST_ATLEAST',
    #    'YOUNG_HOST_TOTAL_ATLEAST', 'YOUNG_HOST_FLEDGED_ATLEAST',
    #    'YOUNG_HOST_DEAD_ATLEAST', 'EGGS_HOST_UNH_ATLEAST',
    #    'CLUTCH_SIZE_PARASITE_ATLEAST', 'EGGS_PARASITE_UNH_ATLEAST',
    #    'YOUNG_PARASITE_TOTAL_ATLEAST', 'YOUNG_PARASITE_FLEDGED_ATLEAST']
    return {}


def insert_counts(raw_data, to_taxon_id, to_event_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')
    # ['LOC_ID', 'LATITUDE', 'LONGITUDE', 'SUBNATIONAL1_CODE', 'ELEVATION_M',
    #    'HEIGHT_M', 'REL_TO_SUBSTRATE', 'SUBSTRATE_CODE',
    #    'CAVITY_ENTRANCE_DIAM_CM', 'ENTRANCE_ORIENTATION', 'HABITAT_CODE_1',
    #    'HABITAT_CODE_2', 'HABITAT_CODE_3', 'ATTEMPT_ID', 'PROJ_PERIOD_ID',
    #    'USER_ID', 'SPECIES_CODE', 'OUTCOME_CODE_LIST', 'FIRST_LAY_DT',
    #    'HATCH_DT', 'FLEDGE_DT', 'CLUTCH_SIZE_HOST_ATLEAST',
    #    'YOUNG_HOST_TOTAL_ATLEAST', 'YOUNG_HOST_FLEDGED_ATLEAST',
    #    'YOUNG_HOST_DEAD_ATLEAST', 'EGGS_HOST_UNH_ATLEAST',
    #    'CLUTCH_SIZE_PARASITE_ATLEAST', 'EGGS_PARASITE_UNH_ATLEAST',
    #    'YOUNG_PARASITE_TOTAL_ATLEAST', 'YOUNG_PARASITE_FLEDGED_ATLEAST']
