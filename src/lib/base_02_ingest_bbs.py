"""Ingest Breed Bird Survey data."""

from os.path import exists
from datetime import date
import subprocess
import pandas as pd
from lib.sqlite_db import SqliteDb as bbs_db
import lib.globals as g


class BaseIngestBbs:
    """Ingest the BBS data."""

    DATASET_ID = 'bbs'
    BBS_PATH = g.DATA_DIR / 'raw' / DATASET_ID
    BBS_DB = str(BBS_PATH / 'breed-bird-survey.sqlite.db')

    def __init__(self, db):
        """Setup."""
        self.db = db
        self.cxn = self.db()
        self.bbs_cxn = None

    def ingest(self):
        """Ingest the BBS data."""
        self._download_bbs_data()

        self.bbs_cxn = bbs_db(self.BBS_DB)

        self.cxn.delete_dataset(self.DATASET_ID)

        taxons = self._select_taxons()
        routes = self._select_routes()
        # weather = self._select_weather()
        # counts = self._select_counts()

        self._insert_dataset()
        self._insert_codes()

        # insert_dataset(cxn)
        # insert_codes(cxn)
        # taxons = select_taxons(cxn)
        # keys = insert_events(cxn)
        # insert_counts(cxn, keys, taxons)
        # db.upevent_places(cxn, self.DATASET_ID)

    def _download_bbs_data(self):
        """Run the script to download the BBS data into an SQLite3 database."""
        cmd = f'retriever install sqlite breed-bird-survey -f {self.BBS_DB}'
        if not exists(self.BBS_DB):
            print('Downloading BBS data')
            subprocess.check_call(cmd, shell=True)

    def _select_taxons(self):
        print('Selecting taxons')
        sql = """SELECT aou, genus, species FROM breed_bird_survey_species"""
        bbs_df = pd.read_sql(sql, self.bbs_cxn.engine)
        bbs_df['genus1'] = bbs_df.genus.str.split().str[0]
        bbs_df['species1'] = bbs_df.species.str.split().str[0]
        bbs_df['sci_name'] = bbs_df.genus1 + ' ' + bbs_df.species1

        sql = """SELECT sci_name, taxon_id FROM taxons"""
        df = pd.read_sql(sql, self.cxn.engine)

        found = bbs_df.sci_name.isin(df.sci_name)
        print(bbs_df[~found])

        return df.taxon_id.to_dict()

    def _select_routes(self):
        print('Getting BBS routes')
        sql = """SELECT * FROM breed_bird_survey_routes"""
        routes = pd.read_sql(sql, self.bbs_cxn.engine).set_index(
            ['statenum', 'route'], verify_integrity=True)
        return routes

    def _select_weather(self):
        print('Getting BBS weather data')
        sql = """SELECT * FROM breed_bird_survey_weather"""
        weather = pd.read_sql(sql, self.bbs_cxn.engine)
        return weather

    def _select_counts(self):
        print('Getting BBS counts')
        sql = """SELECT * FROM breed_bird_survey_counts"""
        counts = pd.read_sql(sql, self.bbs_cxn.engine)
        return counts


# def insert_events(cxn):
#     """Insert event and event detail records."""
#     print('Inserting events')
#
#     sql = """SELECT *
#                FROM bbs.breed_bird_survey_weather
#                JOIN bbs.breed_bird_survey_routes USING (statenum, route)"""
#     events = pd.read_sql(sql, cxn)
#
#     events = events.loc[:, ~events.columns.duplicated()]
#     events = events.rename(
#         columns={'starttime': 'started', 'endtime': 'ended'})
#
#     events = data.add_event_id(events, cxn)
#
#     twenty_five_miles = 1609.344 * 25
#     events['radius'] = twenty_five_miles
#
#     events['dataset_id'] = DATASET_ID
#     events['geohash'] = events.apply(lambda x: geohash2.encode(
#         x.lat, x.lng, precision=7), axis=1)
#     events['day'] = pd.to_datetime(
#         events.loc[:, ['year', 'month', 'day']]).dt.strftime('%j')
#     convert_to_time(events, 'started')
#     convert_to_time(events, 'ended')
#
#     data.insert_events(events, cxn, 'bbs_events')
#
#     return data.make_key_event_id_dict(
#         events, events.statenum, events.route, events.rpid, events.year)
#
#
# def convert_to_time(events, column):
#     """Convert the time field from int hMM format to HH:MM format."""
#     is_na = pd.to_numeric(events[column], errors='coerce').isna()
#     events[column] = events[column].fillna(0).astype(int).astype(str)
#     events[column] = events[column].str.pad(4, fillchar='0')
#     events[column] = pd.to_datetime(
#         events[column], format='%H%M', errors='coerce').dt.strftime('%H:%M')
#     events.loc[is_na, column] = None
#
#
# def insert_counts(cxn, keys, taxons):
#     """Count and count detail records."""
#     print('Inserting counts')
#
#     counts = pd.read_sql('SELECT * FROM bbs.breed_bird_survey_counts', cxn)
#     counts = counts.drop(['record_id'], axis=1).rename(
#         columns={'speciestotal': 'count', 'year': 'bbs_year'})
#
#     counts = data.add_count_id(counts, cxn)
#     counts = data.map_to_taxon_ids(counts, 'aou', taxons)
#     counts = data.map_keys_to_event_ids(
#         counts, keys, counts.statenum, counts.route,
#         counts.rpid, counts.bbs_year)
#
#     data.insert_counts(counts, cxn, 'bbs_counts')

    def _insert_dataset(self):
        """Insert a dataset record."""
        print('Inserting dataset')

        dataset = pd.DataFrame([dict(
            dataset_id=self.DATASET_ID,
            title='North American Breeding Bird Survey (BBS)',
            extracted=str(date.today()),
            version='2016.0',
            url='https://www.pwrc.usgs.gov/bbs/')]).set_index('dataset_id')
        dataset.to_sql(
            'datasets', self.cxn.engine, if_exists='append')

    def _insert_codes(self):
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
