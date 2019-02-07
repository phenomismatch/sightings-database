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

    db.delete_dataset(DATASET_ID)

    to_taxon_id = get_taxa()
    raw_data = get_raw_data(to_taxon_id)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Nestwatch',
        'version': '2018-10-26',
        'url': ''})

    to_place_id = insert_places(raw_data)
    insert_events_and_counts(raw_data, to_place_id, to_taxon_id)


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
    has_taxon_id = raw_data.SPECIES_CODE.isin(to_taxon_id)
    raw_data = raw_data.loc[has_taxon_id, :].copy()
    return raw_data


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


def insert_events_and_counts(raw_data, to_place_id, to_taxon_id):
    """Insert events and counts."""
    log(f'Inserting {DATASET_ID} events and counts')

    event_key = 'LOC_ID ATTEMPT_ID'.split()
    event_dates = 'FIRST_LAY_DT HATCH_DT FLEDGE_DT'.split()
    event_json = """CAVITY_ENTRANCE_DIAM_CM ENTRANCE_ORIENTATION
        HABITAT_CODE_1 HABITAT_CODE_2 HABITAT_CODE_3 PROJ_PERIOD_ID USER_ID
        OUTCOME_CODE_LIST""".split()
    count_fields = 'SPECIES_CODE'.split()
    first_lay_counts = 'CLUTCH_SIZE_HOST_ATLEAST'.split()
    hatch_counts = 'EGGS_HOST_UNH_ATLEAST YOUNG_HOST_TOTAL_ATLEAST'.split()
    fledge_counts = """
        YOUNG_HOST_FLEDGED_ATLEAST YOUNG_HOST_DEAD_ATLEAST""".split()

    print(raw_data.shape)
    # firsts = raw_data.duplicated(subset=['LOC_ID', 'ATTEMPT_ID'], keep='first')
    # new_data = raw_data.groupby(event_key).agg(max)
    print(new_data.shape)

    # raw_data['key'] = tuple(zip(raw_data.LOC_ID, raw_data.ATTEMPT_ID))
    # print(len(raw_data.key.unique()))

    # has_date = raw_data['FIRST_LAY_DT'].notna()
    # has_count = (raw_data['CLUTCH_SIZE_HOST_ATLEAST'].notna()
    #              | raw_data['EGGS_HOST_UNH_ATLEAST'].notna())
    # ]
    return {}
