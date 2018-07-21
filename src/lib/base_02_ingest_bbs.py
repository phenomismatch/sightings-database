"""Ingest Breed Bird Survey data."""

from os.path import exists
from datetime import date
import subprocess
import pandas as pd
import geohash2
from lib.sqlite import Connection
import lib.globals as g


class BaseIngestBbs:
    """Ingest the BBS data."""

    DATASET_ID = 'bbs'
    BBS_PATH = g.DATA_DIR / 'raw' / DATASET_ID
    BBS_DB = str(BBS_PATH / 'breed-bird-survey.sqlite.db')

    def __init__(self, db):
        """Setup."""
        self.cxn = self.db()
        self.bbs_cxn = None

    def ingest(self):
        """Ingest the BBS data."""
        self._download_bbs_data()

        self.bbs_cxn = Connection(self.BBS_DB)

        # db.delete_dataset(cxn, self.DATASET_ID)
        #
        # insert_dataset(cxn)
        # insert_codes(cxn)
        # taxons = select_taxons(cxn)
        # keys = insert_events(cxn)
        # insert_counts(cxn, keys, taxons)
        # db.update_points(cxn, self.DATASET_ID)

    def _download_bbs_data(self):
        """Run the script to download the BBS data into an SQLite3 database."""
        cmd = f'retriever install sqlite breed-bird-survey -f {self.BBS_DB}'
        if not exists(self.BBS_DB):
            print('Downloading BBS data')
            subprocess.check_call(cmd, shell=True)


def insert_events(cxn):
    """Insert event and event detail records."""
    print('Inserting dates')

    sql = """SELECT *
               FROM bbs.breed_bird_survey_weather
               JOIN bbs.breed_bird_survey_routes USING (statenum, route)"""
    dates = pd.read_sql(sql, cxn)

    dates = dates.loc[:, ~dates.columns.duplicated()]
    dates = dates.rename(
        columns={'starttime': 'started', 'endtime': 'ended'})

    dates = data.add_event_id(dates, cxn)

    twenty_five_miles = 1609.344 * 25
    dates['radius'] = twenty_five_miles

    dates['dataset_id'] = DATASET_ID
    dates['geohash'] = dates.apply(lambda x: geohash2.encode(
        x.lat, x.lng, precision=7), axis=1)
    dates['day'] = pd.to_datetime(
        dates.loc[:, ['year', 'month', 'day']]).dt.strftime('%j')
    convert_to_time(dates, 'started')
    convert_to_time(dates, 'ended')

    data.insert_events(dates, cxn, 'bbs_events')

    return data.make_key_event_id_dict(
        dates, dates.statenum, dates.route, dates.rpid, dates.year)


def convert_to_time(dates, column):
    """Convert the time field from int hMM format to HH:MM format."""
    is_na = pd.to_numeric(dates[column], errors='coerce').isna()
    dates[column] = dates[column].fillna(0).astype(int).astype(str)
    dates[column] = dates[column].str.pad(4, fillchar='0')
    dates[column] = pd.to_datetime(
        dates[column], format='%H%M', errors='coerce').dt.strftime('%H:%M')
    dates.loc[is_na, column] = None


def insert_counts(cxn, keys, taxons):
    """Count and count detail records."""
    print('Inserting counts')

    counts = pd.read_sql('SELECT * FROM bbs.breed_bird_survey_counts', cxn)
    counts = counts.drop(['record_id'], axis=1).rename(
        columns={'speciestotal': 'count', 'year': 'bbs_year'})

    counts = data.add_count_id(counts, cxn)
    counts = data.map_to_taxon_ids(counts, 'aou', taxons)
    counts = data.map_keys_to_event_ids(
        counts, keys, counts.statenum, counts.route,
        counts.rpid, counts.bbs_year)

    data.insert_counts(counts, cxn, 'bbs_counts')


def insert_codes(self):
    """Insert BBS code values into the database."""
    print('Inserting codes')

    bcr = pd.read_fwf(
        self.BBS_PATH / 'BCR.txt',
        skiprows=7,
        encoding='ISO-8859-1',
        usecols=[0, 1],
        keep_default_na=False,
        names=['code', 'value'])
    bcr['field'] = 'bcr'

    strata = pd.read_fwf(
        self.BBS_PATH / 'BBSStrata.txt',
        skiprows=16,
        encoding='ISO-8859-1',
        usecols=[0, 1],
        keep_default_na=False,
        names=['code', 'value'])
    strata['field'] = 'strata'

    protocols = pd.read_fwf(
        self.BBS_PATH / 'RunProtocolID.txt',
        skiprows=4,
        encoding='ISO-8859-1',
        colspecs=[(0, 3), (5, 55)],
        names=['code', 'value'])
    protocols['field'] = 'runprotocol'

    descrs = pd.read_fwf(
        self.BBS_PATH / 'RunProtocolID.txt',
        skiprows=4,
        encoding='ISO-8859-1',
        colspecs=[(0, 3), (56, 141)],
        names=['code', 'value'])
    descrs['field'] = 'runprotocoldesc'

    wind = pd.read_fwf(
        self.BBS_PATH / 'weathercodes.txt',
        skiprows=8,
        skipfooter=13,
        encoding='ISO-8859-1',
        colspecs=[(0, 1), (7, 72)],
        keep_default_na=False,
        names=['code', 'value'])
    wind['field'] = 'wind'

    sky = pd.read_fwf(
        self.BBS_PATH / 'weathercodes.txt',
        skiprows=23,
        encoding='ISO-8859-1',
        colspecs=[(0, 1), (6, 56)],
        keep_default_na=False,
        names=['code', 'value'])
    sky['field'] = 'sky'

    states = pd.read_fwf(
        self.BBS_PATH / 'RegionCodes.txt',
        skiprows=11,
        usecols=[1, 2],
        encoding='ISO-8859-1',
        keep_default_na=False,
        names=['code', 'value'])
    states['field'] = 'state'

    types = pd.read_fwf(
        self.BBS_PATH / 'RouteInf.txt',
        skiprows=28,
        skipfooter=13,
        colspecs=[(3, 4), (7, 18)],
        encoding='ISO-8859-1',
        keep_default_na=False,
        names=['code', 'value'])
    types['field'] = 'routetype'

    details = pd.read_fwf(
        self.BBS_PATH / 'RouteInf.txt',
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
    codes.to_sql(
        'bbs_codes', self.cxn.engine, if_exists='replace', index=False)


def select_taxons(cxn):
    """Select taxons and return a dictionary of aou and taxon_ids."""
    print('Selecting taxons')

    sql = """
        SELECT aou, taxon_id
          FROM bbs.breed_bird_survey_species AS b
          JOIN taxons ON (b.genus || ' ' || b.species = sci_name)
        """
    return pd.read_sql(sql, cxn).set_index('aou').taxon_id.to_dict()


def insert_dataset(cxn):
    """Insert a dataset record."""
    print('Inserting dataset')

    dataset = dict(
        dataset_id=DATASET_ID,
        title='North American Breeding Bird Survey (BBS)',
        extracted=str(date.today()),
        version='2016.0',
        url='https://www.pwrc.usgs.gov/bbs/')
    db.insert_dataset(cxn, dataset)
