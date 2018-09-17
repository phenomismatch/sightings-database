"""Utiliites & constants."""

from pathlib import Path


class Dir:
    """Directory constants."""

    data = Path('data')
    temp = data / 'temp'
    interim = data / 'interim'
    external = data / 'external'
    taxonomy = external / 'taxonomy'
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
