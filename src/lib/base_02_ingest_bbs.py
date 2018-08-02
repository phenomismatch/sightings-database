"""Ingest Breed Bird Survey data."""

import gc
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
        self.cxn = self.db(dataset_id=self.DATASET_ID)
        self.bbs_cxn = None

    def ingest(self):
        """Ingest the BBS data."""
        self._download_bbs_data()

        self.bbs_cxn = bbs_db(path=self.BBS_DB)

        self.cxn.bulk_add_setup()
        self.cxn.delete_dataset(self.DATASET_ID)

        aou_to_taxon_id = self._select_taxons()
        routes = self._select_routes()
        weather = self._select_weather()
        counts = self._select_counts()

        self._insert_dataset()
        self._insert_codes()
        route_to_place_id = self._insert_places(routes)
        weather_to_event_id = self._insert_events(weather, route_to_place_id)

        del routes
        del weather
        del route_to_place_id
        gc.collect()

        self._insert_counts(counts, weather_to_event_id, aou_to_taxon_id)
        self.cxn.update_places()
        self.cxn.bulk_add_cleanup()

    def _download_bbs_data(self):
        """Run the script to download the BBS data into an SQLite3 database."""
        cmd = f'retriever install sqlite breed-bird-survey -f {self.BBS_DB}'
        if not exists(self.BBS_DB):
            print('Downloading {self.DATASET_ID} data')
            subprocess.check_call(cmd, shell=True)

    def _select_taxons(self):
        print(f'Selecting taxons for {self.DATASET_ID} counts')
        sql = """SELECT aou, genus, species FROM breed_bird_survey_species"""
        bbs_df = pd.read_sql(sql, self.bbs_cxn.engine)
        bbs_df['genus1'] = bbs_df.genus.str.split().str[0]
        bbs_df['species1'] = bbs_df.species.str.split().str[0]
        bbs_df['sci_name'] = bbs_df.genus1 + ' ' + bbs_df.species1
        bbs_df = bbs_df.loc[:, ['sci_name', 'aou']].set_index('sci_name')

        sql = """SELECT sci_name, taxon_id FROM taxons"""
        df = pd.read_sql(sql, self.cxn.engine).set_index('sci_name')
        df = df.merge(bbs_df, how='inner', left_index=True, right_index=True)

        return df.set_index('aou').taxon_id.to_dict()

    def _select_routes(self):
        print(f'Getting {self.DATASET_ID} routes')
        sql = """SELECT * FROM breed_bird_survey_routes"""
        routes = pd.read_sql(sql, self.bbs_cxn.engine).set_index(
            ['statenum', 'route'], verify_integrity=True)
        return routes

    def _select_weather(self):
        print(f'Getting {self.DATASET_ID} weather data')
        sql = """SELECT * FROM breed_bird_survey_weather"""
        weather = pd.read_sql(sql, self.bbs_cxn.engine)
        return weather

    def _select_counts(self):
        print(f'Getting {self.DATASET_ID} counts')
        sql = """SELECT * FROM breed_bird_survey_counts"""
        counts = pd.read_sql(sql, self.bbs_cxn.engine)
        return counts

    def _insert_places(self, places):
        print(f'Inserting {self.DATASET_ID} places')
        places = places.reset_index().rename(
            columns={'latitude': 'lat', 'longitude': 'lng'})
        places['dataset_id'] = self.DATASET_ID
        places['radius'] = 1609.344 * 25  # twenty-five miles in meters
        places = self.cxn.add_place_id(places)

        self.cxn.insert_places(places)
        return places.reset_index().set_index(
            ['statenum', 'route']).place_id.to_dict()

    def _insert_events(self, events, route_to_place_id):
        print(f'Inserting {self.DATASET_ID} events')
        events = events.rename(
            columns={'starttime': 'started', 'endtime': 'ended'})
        events['day'] = pd.to_datetime(
            events.loc[:, ['year', 'month', 'day']]).dt.strftime('%j')
        events['place_id'] = events.apply(
            lambda x: route_to_place_id[(x.statenum, x.route)], axis=1)
        self._convert_to_time(events, 'started')
        self._convert_to_time(events, 'ended')
        events = self.cxn.add_event_id(events)
        self.cxn.insert_events(events)
        return events.reset_index().set_index(
            ['statenum', 'route', 'rpid', 'year']).event_id.to_dict()

    def _insert_counts(self, counts, weather_to_event_id, aou_to_taxon_id):
        print(f'Inserting {self.DATASET_ID} counts')
        counts = counts.rename(columns={'speciestotal': 'count'})
        counts['taxon_id'] = counts.aou.map(aou_to_taxon_id)
        counts = counts.loc[counts.taxon_id.notna(), :]
        counts.taxon_id = counts.taxon_id.astype(int)
        counts['key'] = tuple(zip(
            counts.statenum, counts.route, counts.rpid, counts.year))
        counts['event_id'] = counts.key.map(weather_to_event_id)
        counts.drop(['key', 'record_id'], axis=1)
        counts = self.cxn.add_count_id(counts)
        self.cxn.insert_counts(counts)

    @staticmethod
    def _convert_to_time(df, column):
        """Convert the time field from int hMM format to HH:MM format."""
        is_na = pd.to_numeric(df[column], errors='coerce').isna()
        df[column] = df[column].fillna(0).astype(int).astype(str)
        df[column] = df[column].str.pad(4, fillchar='0')
        df[column] = pd.to_datetime(
            df[column], format='%H%M', errors='coerce').dt.time
        df.loc[is_na, column] = None

    def _insert_dataset(self):
        """Insert a dataset record."""
        print(f'Inserting {self.DATASET_ID} dataset')

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
        print(f'Inserting {self.DATASET_ID} codes')

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
        codes = self.cxn.add_code_id(codes)
        codes.to_sql('bbs_codes', self.cxn.engine, if_exists='replace')
