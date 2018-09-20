"""Utiliites & constants."""

from pathlib import Path
from datetime import datetime
import pandas as pd
import lib.db as db


def log(msg):
    """Print a status message to the screen."""
    msg = f'{datetime.now()} {msg}'
    print(msg)


def filter_lat_lng(df, lat=(-90.0, 90.0), lng=(-180.0, 180.0)):
    """Remove bad latitudes and longitudes."""
    df.lat = pd.to_numeric(
        df.lat, errors='coerce').fillna(9999.9).astype(float)
    df.lng = pd.to_numeric(
        df.lng, errors='coerce').fillna(9999.9).astype(float)
    good_lat = df.lat.between(lat[0], lat[1])
    good_lng = df.lng.between(lng[0], lng[1])

    return df.loc[good_lat & good_lng, :]


def drop_duplicate_taxons(taxons):
    """Update the taxons dataframe and add them to the taxons CSV file."""
    cxn = db.connect()
    existing = pd.read_sql('SELECT sci_name, taxon_id FROM taxons', cxn)
    existing = existing.set_index('sci_name').taxon_id.to_dict()
    in_existing = taxons.sci_name.isin(existing)
    return taxons.loc[~in_existing, :].copy()


def add_taxon_genera_records(taxons):
    """Create genera records."""
    genera = taxons.groupby('genus').first().reset_index()
    genera.sci_name = genera.genus + ' sp.'
    genera.common_name = ''
    taxons = pd.concat([taxons, genera], sort=True)
    return taxons


class Dir:
    """Directory constants."""

    data = Path('data')
    raw = data / 'raw'
    temp = data / 'temp'
    interim = data / 'interim'
    external = data / 'external'
    taxonomy = raw / 'taxonomy'
    sql = Path('lib') / 'sql'


class Bbs:
    """Dataset constants."""

    dataset_id = 'bbs'
    path = Dir.data / 'raw' / dataset_id
    db = str(path / 'breed-bird-survey.sqlite.db')


class Caterpillar:
    """Dataset constants."""

    dataset_id = 'caterpillar'
    path = Dir.data / 'raw' / dataset_id
    prefix = '2018-09-16'
    site_csv = path / f'{prefix}_Site.csv'
    plant_csv = path / f'{prefix}_Plant.csv'
    survey_csv = path / f'{prefix}_Survey.csv'
    sightings_csv = path / f'{prefix}_ArthropodSighting.csv'


class Clements:
    """Dataset constants."""

    dataset_id = 'clements'
    csv = Dir.taxonomy / 'Clements-Checklist-v2017-August-2017_2.csv'


class Countries:
    """Dataset constants."""

    csv = Dir.external / 'misc' / 'ISO_3166-1_country_codes.csv'


class Ebird:
    """Dataset constants."""

    dataset_id = 'ebird'
    path = Dir.data / 'raw' / dataset_id
    csv = path / 'ebd_relFeb-2018.txt'


class Maps:
    """Dataset constants."""

    dataset_id = 'maps'
    path = Dir.data / 'raw' / dataset_id
    list_ = 'LIST17'
    bands = '1117BAND'
    effort = '1117EF'
    stations = 'STATIONS'


class Naba:
    """Dataset constants."""

    dataset_id = 'naba'
    path = Dir.data / 'raw' / dataset_id
    csv = path / 'NABA_JULY4_V2.csv'


class Pollard:
    """Dataset constants."""

    dataset_id = 'pollard'
    path = Dir.data / 'raw' / dataset_id
    place_csv = path / 'Pollard_locations.csv'
    data_csv = path / 'pollardbase_example_201802.csv'


class TargetBirds:
    """Dataset constants."""

    csv = Dir.taxonomy / 'target_birds.csv'
