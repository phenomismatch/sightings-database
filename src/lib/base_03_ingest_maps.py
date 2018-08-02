"""Ingest Monitoring Avian Productivity and Survivorship data."""

import os
from datetime import date
import pandas as pd
from simpledbf import Dbf5
import lib.globals as g
import lib.data as data


class BaseIngestMaps:
    """Ingest the MAPS data."""

    DATASET_ID = 'maps'
    MAPS_PATH = g.DATA_DIR / 'raw' / DATASET_ID
    LIST = 'LIST17'
    BANDS = '1117BAND'
    EFFORT = '1117EF'
    STATIONS = 'STATIONS'

    def __init__(self, db):
        """Setup."""
        self.db = db
        self.cxn = self.db(dataset_id=self.DATASET_ID)
        self.bbs_cxn = None

    def ingest(self):
        """Ingest the data."""
        self._convert_dbf_to_csv(self.LIST)
        self._convert_dbf_to_csv(self.BANDS)
        self._convert_dbf_to_csv(self.EFFORT)

        self.cxn.bulk_add_setup()
        self.cxn.delete_dataset()

        to_taxon_id = self._get_raw_taxons()
        raw_places = self._get_raw_places()
        raw_events = self._get_raw_events()
        raw_counts = self._get_raw_counts()

        self._insert_dataset()
        self._insert_codes()
        to_place_id = self._insert_places(raw_places)
        to_event_id = self._insert_events(raw_events, to_place_id)
        self._insert_counts(raw_counts, to_event_id, to_taxon_id)
        self.cxn.update_places()

        self.cxn.bulk_add_cleanup()

    def _convert_dbf_to_csv(self, file_name):
        csv_file = self.MAPS_PATH / f'{file_name}.csv'
        dbf_file = self.MAPS_PATH / f'{file_name}.DBF'
        if not os.path.exists(csv_file):
            print(f'Converting {dbf_file} files to {csv_file}')
            dbf = Dbf5(dbf_file)
            df = dbf.to_dataframe()
            df.to_csv(csv_file, index=False)

    def _get_raw_taxons(self):
        print(f'Getting {self.DATASET_ID} raw taxon data')
        raw_taxons = pd.read_csv(self.MAPS_PATH / f'{self.LIST}.csv')
        raw_taxons = raw_taxons.loc[:, ['SCINAME', 'SPEC']]
        raw_taxons = raw_taxons.set_index('SCINAME')
        sql = """SELECT sci_name, taxon_id FROM taxons"""
        taxons = pd.read_sql(sql, self.cxn.engine).set_index('sci_name')
        taxons = taxons.merge(
            raw_taxons, how='inner', left_index=True, right_index=True)
        return taxons.set_index('SPEC').taxon_id.to_dict()

    def _get_raw_places(self):
        print(f'Getting {self.DATASET_ID} raw place data')

        keep = """STATION LOC STA STA2 NAME LHOLD HOLDCERT O NEARTOWN COUNTY
            STATE US REGION BLOCK LATITUDE LONGITUDE PRECISION SOURCE DATUM
            DECLAT DECLNG NAD83 ELEV STRATUM BCR HABITAT REG PASSED""".split()

        csv_file = self.MAPS_PATH / f'{self.STATIONS}.csv'
        raw_places = pd.read_csv(csv_file, dtype='unicode')
        raw_places = raw_places.drop(columns=[
            c for c in raw_places.columns if c not in keep], axis=1)

        raw_places = raw_places.rename(
            columns={'DECLAT': 'lat', 'DECLNG': 'lng'})

        radii = {
            '01S': 30.92,
            '10S': 30.92 * 10,
            'BLK': 111.32 * 1000 * 10,
            '01M': 111.32 * 1000,
            '05S': 30.92 * 5,
            '10M': 111.32 * 1000 * 10}
        raw_places['radius'] = raw_places.PRECISION.map(radii)

        raw_places.STA = raw_places.STA.fillna('').astype(str)
        good_sta = raw_places.STA.str.len() > 3
        raw_places = raw_places.loc[good_sta, :]

        raw_places = data.filter_lat_lng(raw_places)

        return raw_places

    def _get_raw_events(self):
        print(f'Getting {self.DATASET_ID} raw event data')
        csv_file = self.MAPS_PATH / f'{self.EFFORT}.csv'
        raw_events = pd.read_csv(csv_file, dtype='unicode')
        df = raw_events.groupby(['STA', 'DATE'])
        df = df.agg({'START': min, 'END': max})
        print(df.shape)
        df = raw_events.groupby(['STA', 'DATE', 'STATION', 'IP', 'LENGTH'])
        df = df.agg({'START': min, 'END': max})
        print(df.shape)
        import sys
        sys.exit()
        df = df.rename(columns={'START': 'started', 'END': 'ended'})
        # raw_events = raw_events.reset_index()
        self._convert_to_time(raw_events, 'started')
        self._convert_to_time(raw_events, 'ended')
        return raw_events

    def _get_raw_counts(self):
        print(f'Getting {self.DATASET_ID} raw count data')
        csv_file = self.MAPS_PATH / f'{self.BANDS}.csv'
        raw_counts = pd.read_csv(csv_file, dtype='unicode')
        return raw_counts

    def _insert_places(self, raw_places):
        print(f'Inserting {self.DATASET_ID} places')
        places = raw_places.reset_index()
        places['dataset_id'] = self.DATASET_ID
        places = self.cxn.add_place_id(places)
        self.cxn.insert_places(places)
        return places.reset_index().set_index(
            'STA', verify_integrity=True).place_id.to_dict()

    def _insert_events(self, raw_events, to_place_id):
        print(f'Inserting {self.DATASET_ID} events')
        events = self.cxn.add_event_id(raw_events)
        print(events.head())
        print(events.columns)
        events['place_id'] = events.STA.map(to_place_id)
        events.DATE = pd.to_datetime(events.DATE)
        events['year'] = events.DATE.dt.strftime('%Y')
        events['day'] = events.DATE.dt.strftime('%j')
        self.cxn.insert_events(events)
        return events.reset_index().set_index(
            ['STA', 'DATE']).event_id.to_dict()

    def _insert_counts(self, counts, effort_to_event_id, to_taxon_id):
        print(f'Inserting {self.DATASET_ID} counts')
#     counts = data.map_keys_to_event_ids(
#         counts, keys, counts.STA, pd.to_datetime(counts.DATE))
#
#     counts = data.add_count_id(counts, cxn)
#     counts['count'] = 1
#
#     data.insert_counts(counts, cxn, 'maps_counts')

    @staticmethod
    def _convert_to_time(df, column):
        """Convert the time field from string HHm format to HH:MM format."""
        is_na = df[column].isna()
        df[column] = df[column].fillna('0').astype(str)
        df[column] = df[column].str.pad(4, fillchar='0', side='right')
        df[column] = pd.to_datetime(
            df[column], format='%H%M', errors='coerce').dt.strftime('%H:%M')
        df.loc[is_na, column] = None

    def _insert_dataset(self):
        print(f'Inserting {self.DATASET_ID} dataset')

        dataset = pd.DataFrame([dict(
            dataset_id=self.DATASET_ID,
            title='MAPS: Monitoring Avian Productivity and Survivorship',
            extracted=str(date.today()),
            version='2017.0',
            url='https://www.birdpop.org/pages/maps.php')])
        dataset.set_index('dataset_id').to_sql(
            'datasets', self.cxn.engine, if_exists='append')

    def _insert_codes(self):
        print(f'Inserting {self.DATASET_ID} codes')

        codes = pd.read_csv(self.MAPS_PATH / 'maps_codes.csv')
        codes = self.cxn.add_code_id(codes)
        codes.to_sql('maps_codes', self.cxn.engine, if_exists='replace')
