"""Ingest Caterpillar Counts data."""

from pathlib import Path
import pandas as pd
import lib.db as db
import lib.util as util
from lib.util import log


DATASET_ID = 'caterpillar'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
FILE_DATE = '2018-09-19'
SIGHTINGS_CSV = RAW_DIR / f'{FILE_DATE}_ArthropodSighting.csv'
SURVEY_CSV = RAW_DIR / f'{FILE_DATE}_Survey.csv'
PLANT_CSV = RAW_DIR / f'{FILE_DATE}_Plant.csv'
SITE_CSV = RAW_DIR / f'{FILE_DATE}_Site.csv'


def ingest():
    """Ingest the data."""
    db.delete_dataset(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'Caterpillar Counts',
        'version': '2018-09-18',
        'url': ('https://caterpillarscount.unc.edu/'
                'iuFYr1xREQOp2ioB5MHvnCTY39UHv2/')})

    to_taxon_id = insert_taxons()
    to_place_id = insert_places()
    to_event_id = insert_events(to_place_id)
    insert_counts(to_event_id, to_taxon_id)


def insert_taxons():
    """Insert taxa."""
    log(f'Inserting {DATASET_ID} taxa')

    cxn = db.connect()

    taxa = pd.read_csv(SIGHTINGS_CSV, encoding='ISO-8859-1')

    firsts = taxa.Group.duplicated(keep='first')
    taxa = taxa.loc[firsts, ['Group']].copy()
    taxa = taxa.rename(columns={'Group': 'group'})

    taxa['sci_name'] = taxa.group
    taxa['genus'] = None
    taxa['dataset_id'] = DATASET_ID
    taxa['class'] = None
    taxa['order'] = None
    taxa['family'] = None
    taxa['target'] = None
    taxa['common_name'] = ''

    # taxa = util.drop_duplicate_taxons(taxa)
    taxa['taxon_id'] = db.get_ids(taxa, 'taxa')
    taxa.taxon_id = taxa.taxon_id.astype(int)

    taxa.to_sql('taxa', cxn, if_exists='append', index=False)

    sql = """SELECT sci_name, taxon_id FROM taxa WHERE "group" IS NOT NULL"""
    return pd.read_sql(sql, cxn).set_index('sci_name').taxon_id.to_dict()


def insert_places():
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    raw_places = pd.read_csv(SITE_CSV, encoding='ISO-8859-1')

    places = pd.DataFrame()

    raw_places['place_id'] = db.get_ids(raw_places, 'places')
    places['place_id'] = raw_places['place_id']

    places['dataset_id'] = DATASET_ID

    places['lng'] = raw_places['Longitude']

    places['lat'] = raw_places['Latitude']

    fields = """ID Name Description Region""".split()
    places['place_json'] = util.json_object(raw_places, fields)

    places.to_sql('places', db.connect(), if_exists='append', index=False)

    # Build dictionary to map events to place IDs
    return raw_places.set_index('ID').place_id.to_dict()


def insert_events(to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    raw_plants = pd.read_csv(PLANT_CSV, encoding='ISO-8859-1')
    raw_surveys = pd.read_csv(SURVEY_CSV, encoding='ISO-8859-1')
    raw_events = pd.merge(
        left=raw_surveys, right=raw_plants, left_on='PlantFK', right_on='ID',
        suffixes=['_survey', '_plant'])

    events = pd.DataFrame()

    raw_events['event_id'] = db.get_ids(raw_events, 'events')
    events['event_id'] = raw_events['event_id']

    events['place_id'] = raw_events.SiteFK.map(to_place_id)

    raw_events['raw_date'] = pd.to_datetime(
        raw_events.LocalDate, format='%Y-%m-%d', errors='coerce')
    events['year'] = raw_events.raw_date.dt.strftime('%Y')
    events['day'] = raw_events.raw_date.dt.strftime('%j')

    events['started'] = raw_events['LocalTime'].str[:5]

    events['ended'] = None

    fields = """ID_survey ID_plant SiteFK Circle Orientation Code Species
        SubmissionTimestamp LocalDate LocalTime ObservationMethod Notes
        WetLeaves PlantSpecies NumberOfLeaves AverageLeafLength HerbivoryScore
        SubmittedThroughApp MinimumTemperature MaximumTemperature""".split()
    events['event_json'] = util.json_object(raw_events, fields, DATASET_ID)

    events.to_sql('events', db.connect(), if_exists='append', index=False)

    # Build dictionary to map events to place IDs
    return raw_events.set_index('ID_survey').event_id.to_dict()


def insert_counts(to_event_id, to_taxon_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    raw_counts = pd.read_csv(SIGHTINGS_CSV, encoding='ISO-8859-1')

    counts = pd.DataFrame()
    counts['count_id'] = db.get_ids(raw_counts, 'counts')

    counts['event_id'] = raw_counts.SurveyFK.map(to_event_id)
    counts['taxon_id'] = raw_counts.Group.map(to_taxon_id)

    counts['count'] = raw_counts.Quantity

    fields = """ID SurveyFK Length PhotoURL Notes Hairy Rolled
        Tented""".split()
    counts['count_json'] = util.json_object(raw_counts, fields, DATASET_ID)

    has_event_id = counts.event_id.notna()
    has_taxon_id = counts.taxon_id.notna()
    has_count = counts['count'].notna()
    counts = counts.loc[has_event_id & has_taxon_id & has_count, :]

    counts.to_sql('counts', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
