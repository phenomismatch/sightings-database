"""Ingest Breed Bird Survey data."""

from os.path import exists
from datetime import date
import subprocess
import pandas as pd
from lib.sqlite_db import SqliteDb as bbs_db
import lib.globals as g


class BaseIngestBbs:
    """Ingest BBS data."""

    DATASET_ID = 'bbs'
    BBS_PATH = g.DATA_DIR / 'raw' / DATASET_ID
    BBS_DB = str(BBS_PATH / 'breed-bird-survey.sqlite.db')

    def __init__(self, db):
        """Setup."""
        self.db = db
        self.cxn = self.db(dataset_id=self.DATASET_ID)
        self.bbs_cxn = None

    def ingest(self):
        """Ingest the data."""
        self._download_bbs_data()

        self.bbs_cxn = bbs_db(path=self.BBS_DB)

        self.cxn.bulk_add_setup()
        self.cxn.delete_dataset()

        to_taxon_id = self._select_raw_taxons()
        raw_places = self._select_raw_places()
        raw_events = self._select_raw_events()
        raw_counts = self._select_raw_counts()

        self._insert_dataset()
        self._insert_codes()
        to_place_id = self._insert_places(raw_places)
        to_event_id = self._insert_events(raw_events, to_place_id)
        self._insert_counts(raw_counts, to_event_id, to_taxon_id)
        self.cxn.update_places()

        self.cxn.bulk_add_cleanup()

    def _download_bbs_data(self):
        """Run the script to download the BBS data into an SQLite3 database."""
        cmd = f'retriever install sqlite breed-bird-survey -f {self.BBS_DB}'
        if not exists(self.BBS_DB):
            print('Downloading {self.DATASET_ID} data')
            subprocess.check_call(cmd, shell=True)

    def _select_raw_taxons(self):
        print(f'Getting {self.DATASET_ID} raw taxon data')
        sql = """SELECT aou, genus, species FROM breed_bird_survey_species"""
        raw_taxons = pd.read_sql(sql, self.bbs_cxn.engine)
        raw_taxons['genus1'] = raw_taxons.genus.str.split().str[0]
        raw_taxons['species1'] = raw_taxons.species.str.split().str[0]
        raw_taxons['sci_name'] = raw_taxons.genus1 + ' ' + raw_taxons.species1
        raw_taxons = raw_taxons.loc[:, ['sci_name', 'aou']]
        raw_taxons = raw_taxons.set_index('sci_name')

        sql = """SELECT sci_name, taxon_id FROM taxons"""
        taxons = pd.read_sql(sql, self.cxn.engine).set_index('sci_name')
        taxons = taxons.merge(
            raw_taxons, how='inner', left_index=True, right_index=True)

        return taxons.set_index('aou').taxon_id.to_dict()

    def _select_raw_places(self):
        print(f'Getting {self.DATASET_ID} raw place data')
        sql = """SELECT * FROM breed_bird_survey_routes"""
        raw_places = pd.read_sql(sql, self.bbs_cxn.engine).set_index(
            ['statenum', 'route'], verify_integrity=True)
        return raw_places

    def _select_raw_events(self):
        print(f'Getting {self.DATASET_ID} raw event data')
        sql = """SELECT * FROM breed_bird_survey_weather"""
        raw_events = pd.read_sql(sql, self.bbs_cxn.engine)
        return raw_events

    def _select_raw_counts(self):
        print(f'Getting {self.DATASET_ID} raw count data')
        sql = """SELECT * FROM breed_bird_survey_counts"""
        raw_counts = pd.read_sql(sql, self.bbs_cxn.engine)
        return raw_counts

    def _insert_places(self, raw_places):
        print(f'Inserting {self.DATASET_ID} places')
        places = raw_places.reset_index()
        places['dataset_id'] = self.DATASET_ID
        places = self._set_lng(places)
        places = self._set_lat(places)
        places['radius'] = 1609.344 * 25  # twenty-five miles in meters
        places = self.cxn.add_place_id(places)

        self.cxn.insert_places(places)
        return places.reset_index().set_index(
            ['statenum', 'route']).place_id.to_dict()

    def _set_lng(self, places):
        return places.rename(columns={'longitude': 'lng'})

    def _set_lat(self, places):
        return places.rename(columns={'latitude': 'lat'})

    def _set_radius(self, places):
        places['radius'] = 1609.344 * 25  # twenty-five miles in meters
        return places

    def _insert_events(self, raw_events, to_place_id):
        print(f'Inserting {self.DATASET_ID} events')
        events = raw_events.rename(
            columns={'starttime': 'started', 'endtime': 'ended'})
        events['day'] = pd.to_datetime(
            events.loc[:, ['year', 'month', 'day']]).dt.strftime('%j')
        events['key'] = tuple(zip(events.statenum, events.route))
        events['place_id'] = events.key.map(to_place_id)
        events = events.drop(['key'], axis=1)
        self._convert_to_time(events, 'started')
        self._convert_to_time(events, 'ended')
        events = self.cxn.add_event_id(events)
        self.cxn.insert_events(events)
        return events.reset_index().set_index(
            ['statenum', 'route', 'rpid', 'year']).event_id.to_dict()

    def _insert_counts(self, raw_counts, to_event_id, to_taxon_id):
        print(f'Inserting {self.DATASET_ID} counts')
        counts = raw_counts.rename(columns={'speciestotal': 'count'})
        counts['taxon_id'] = counts.aou.map(to_taxon_id)
        counts = counts.loc[counts.taxon_id.notna(), :]
        counts.taxon_id = counts.taxon_id.astype(int)
        counts['key'] = tuple(zip(
            counts.statenum, counts.route, counts.rpid, counts.year))
        counts['event_id'] = counts.key.map(to_event_id)
        counts = counts.drop(['key', 'record_id'], axis=1)
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
        print(f'Inserting {self.DATASET_ID} dataset')
        dataset = pd.DataFrame([dict(
            dataset_id=self.DATASET_ID,
            title='North American Breeding Bird Survey (BBS)',
            extracted=str(date.today()),
            version='2016.0',
            url='https://www.pwrc.usgs.gov/bbs/')])
        dataset.set_index('dataset_id').to_sql(
            'datasets', self.cxn.engine, if_exists='append')

    def _insert_codes(self):
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
