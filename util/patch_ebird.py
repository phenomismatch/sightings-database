"""Add target species missing from eBird data."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util
from lib.util import log


DATASET_ID = 'ebird'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
RAW_CSV = 'ebd_relDec-2018.txt.gz'

TARGETS = ['Ammospiza caudacuta', 'Ammospiza nelsoni']


def ingest():
    """Ingest eBird data."""
    to_taxon_id = get_taxa()

    chunk = 1_000_000
    reader = pd.read_csv(
        RAW_DIR / RAW_CSV,
        delimiter='\t',
        quoting=3,
        chunksize=chunk,
        compression='infer',
        dtype='unicode')

    to_event_id = get_events()

    for i, raw_data in enumerate(reader, 1):
        log(f'Processing {DATASET_ID} chunk {i * chunk:,}')
        util.normalize_columns_names(raw_data)
        insert_counts(raw_data, to_event_id, to_taxon_id)


def get_taxa():
    """Build a dictionary of scientific names and taxon_ids."""
    log(f'Getting {DATASET_ID} taxa')
    sql = """SELECT taxon_id, sci_name
               FROM taxa
              WHERE sci_name IN ({})"""
    sql = sql.format(','.join([f"'{x}'" for x in TARGETS]))
    taxa = pd.read_sql(sql, db.connect())
    return taxa.set_index('sci_name').taxon_id.to_dict()


def get_events():
    """Get events."""
    log(f'Getting {DATASET_ID} events')

    sql = f"""SELECT
        json_extract(event_json, '$.SAMPLING_EVENT_IDENTIFIER')
            AS SAMPLING_EVENT_IDENTIFIER,
        event_id
        FROM events
        WHERE DATASET_ID = '{DATASET_ID}'"""

    events = pd.read_sql(sql, db.connect())

    return events.set_index('SAMPLING_EVENT_IDENTIFIER').event_id.to_dict()


def insert_counts(counts, to_event_id, to_taxon_id):
    """Insert counts."""
    in_species = counts['SCIENTIFIC_NAME'].isin(to_taxon_id)
    has_event = counts['SAMPLING_EVENT_IDENTIFIER'].isin(to_event_id)
    counts = counts.loc[in_species & has_event, :].copy()

    if counts.shape[0] == 0:
        return

    counts['count_id'] = db.get_ids(counts, 'counts')
    counts['event_id'] = counts.SAMPLING_EVENT_IDENTIFIER.map(to_event_id)
    counts['taxon_id'] = counts.SCIENTIFIC_NAME.map(to_taxon_id)
    counts['dataset_id'] = DATASET_ID

    counts = counts.rename(columns={'OBSERVATION_COUNT': 'count'})
    counts.loc[counts['count'] == 'X', 'count'] = '-1'
    counts['count'] = pd.to_numeric(counts['count'], errors='coerce')

    fields = """SCIENTIFIC_NAME GLOBAL_UNIQUE_IDENTIFIER LAST_EDITED_DATE
        TAXONOMIC_ORDER CATEGORY SUBSPECIES_SCIENTIFIC_NAME
        BREEDING_BIRD_ATLAS_CODE BREEDING_BIRD_ATLAS_CATEGORY AGE_SEX
        OBSERVER_ID HAS_MEDIA SPECIES_COMMENTS""".split()
    counts['count_json'] = util.json_object(counts, fields)

    print(counts.shape)

    counts.loc[:, db.COUNT_FIELDS].to_sql(
        'counts', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()

#SELECT * FROM counts WHERE dataset_id = 'ebird' AND taxon_id in (30923, 30928)
