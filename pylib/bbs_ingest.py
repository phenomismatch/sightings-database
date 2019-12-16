"""Ingest Breed Bird Survey data."""

import re
from pathlib import Path
import pandas as pd
from . import db
from . import util
from .util import log


DATASET_ID = 'bbs'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
BBS_DB = str(RAW_DIR / 'breed-bird-survey.sqlite.db')


def ingest():
    """Ingest Breed Bird Survey data."""
    db.delete_dataset_records(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'North American Breeding Bird Survey (BBS)',
        'version': '2016.0',
        'url': 'https://www.pwrc.usgs.gov/bbs/'})

    to_taxon_id = insert_taxa()
    to_place_id = insert_places()
    to_event_id = insert_events(to_place_id)
    insert_counts(to_event_id, to_taxon_id)


def insert_taxa():
    """Insert taxa."""
    log(f'Inserting {DATASET_ID} taxa')

    sql = """SELECT * FROM breed_bird_survey_species"""
    raw_taxa = pd.read_sql(sql, db.connect(BBS_DB))
    raw_taxa['sci_name'] = raw_taxa.genus + ' ' + raw_taxa.species
    raw_taxa.sci_name = raw_taxa.sci_name.str.split().str.join(' ')
    raw_taxa.sci_name = raw_taxa.sci_name.apply(
        lambda x: re.sub(r'\s*/\s*', '/', x))
    raw_taxa.rename(
        columns={'sporder': 'order', 'english_common_name': 'common_name'},
        inplace=True)

    taxa = raw_taxa.copy()
    taxa = db.drop_duplicate_taxa(taxa)
    taxa['taxon_id'] = db.get_ids(taxa, 'taxa')
    taxa.taxon_id = taxa.taxon_id.astype(int)
    taxa['class'] = 'aves'
    taxa['group'] = None
    taxa['target'] = None
    taxa['category'] = None
    fields = 'species_id aou french_common_name spanish_common_name'.split()
    taxa['taxon_json'] = util.json_object(taxa, fields)
    taxa.loc[:, db.TAXON_FIELDS].to_sql(
        'taxa', db.connect(), if_exists='append', index=False)

    raw_taxa = raw_taxa.set_index('sci_name')
    sql = """SELECT * FROM taxa"""
    taxa = pd.read_sql(sql, db.connect()).set_index('sci_name')
    taxa = taxa.merge(raw_taxa, how='inner', left_index=True, right_index=True)
    db.update_taxa_json(taxa, fields)

    to_taxon_id = taxa.set_index('aou').taxon_id.to_dict()
    return to_taxon_id


def insert_places():
    """Insert places."""
    log(f'Inserting {DATASET_ID} places')

    sql = """SELECT * FROM breed_bird_survey_routes"""
    raw_places = pd.read_sql(sql, db.connect(BBS_DB))

    places = pd.DataFrame()
    raw_places['place_id'] = db.get_ids(raw_places, 'places')
    places['place_id'] = raw_places['place_id']
    places['dataset_id'] = DATASET_ID
    places['lng'] = raw_places['longitude']
    places['lat'] = raw_places['latitude']
    places['radius'] = 1609.344 * 25  # twenty-five miles in meters

    fields = """countrynum statenum route routename active stratum bcr
        routetypeid routetypedetailid""".split()
    places['place_json'] = util.json_object(raw_places, fields)

    places.to_sql('places', db.connect(), if_exists='append', index=False)

    # Build dictionary to map events to place IDs
    return raw_places.set_index(['statenum', 'route']).place_id.to_dict()


def insert_events(to_place_id):
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    sql = """SELECT * FROM breed_bird_survey_weather"""
    raw_events = pd.read_sql(sql, db.connect(BBS_DB))

    events = pd.DataFrame()

    raw_events['event_id'] = db.get_ids(raw_events, 'events')
    events['event_id'] = raw_events['event_id']
    events['dataset_id'] = DATASET_ID
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
    events['event_json'] = util.json_object(raw_events, fields)
    events.to_sql('events', db.connect(), if_exists='append', index=False)

    # Build dictionary to map events to place IDs
    return raw_events.set_index(
        ['statenum', 'route', 'rpid', 'year']).event_id.to_dict()


def convert_to_time(dfm, column):
    """Convert the time field from int hMM format to HH:MM format."""
    is_na = pd.to_numeric(dfm[column], errors='coerce').isna()
    dfm[column] = dfm[column].fillna(0).astype(int).astype(str)
    dfm[column] = dfm[column].str.pad(4, fillchar='0')
    dfm[column] = pd.to_datetime(
        dfm[column], format='%H%M', errors='coerce').dt.strftime('%H:%M')
    dfm.loc[is_na, column] = ''


def insert_counts(to_event_id, to_taxon_id):
    """Insert counts."""
    log(f'Inserting {DATASET_ID} counts')

    sql = """SELECT * FROM breed_bird_survey_counts"""
    raw_counts = pd.read_sql(sql, db.connect(BBS_DB))

    raw_counts['taxon_id'] = raw_counts.aou.map(to_taxon_id)
    counts = pd.DataFrame()
    counts['count_id'] = db.get_ids(raw_counts, 'counts')
    counts['dataset_id'] = DATASET_ID
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
    counts['count_json'] = util.json_object(raw_counts, fields)

    counts = counts[counts.event_id.notna() & counts.taxon_id.notna()]
    counts.to_sql('counts', db.connect(), if_exists='append', index=False)


if __name__ == '__main__':
    ingest()
