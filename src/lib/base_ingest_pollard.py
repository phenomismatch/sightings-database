"""Ingest Pollard data."""

import re
from datetime import date
import pandas as pd
import lib.data as data
import lib.globals as g


class BaseIngestPollard:
    """Ingest the MAPS data."""

    DATASET_ID = 'pollard'
    POLLARD_PATH = g.DATA_DIR / 'raw' / DATASET_ID

    PLACE_RENAMES = {'long': 'lng', 'Land Owner': 'Land_Owner'}
    RAW_DATA_RENAMES = {
        'Scientific Name': 'sci_name',
        'Species': 'common_name',
        'Start time': 'Start_time',
        'End time': 'End_time',
        'Was the survey completed?': 'Was_the_survey_completed',
        'Monitoring Program': 'Monitoring_Program',
        'Temperature (end)': 'Temperature_end',
        'Sky (end)': 'Sky_end',
        'Wind (end)': 'Wind_end',
        'A-key': 'A_key',
        'B-key': 'B_key',
        'C-key': 'C_key',
        'D-key': 'D_key',
        'E-key': 'E_key',
        'Observer/Spotter': '',
        'Other participants': '',
        'Recorder/Scribe': '',
        'Taxon as reported': '',
        'Total': 'count'}

    PLACE_KEYS = ['Site', 'Route']
    PLACE_COLUMNS = '''
        lat lng Site Route County State Land_Owner transect_id Route_Poin
        Route_Po_1 Route_Po_2 CLIMDIV_ID CD_sub CD_Name ST PRE_MEAN PRE_STD
        TMP_MEAN TMP_STD'''.split()

    EVENT_COLUMNS = '''
        Site Route County State Start_time End_time Duration Survey Temp Sky
        Wind Archived Was_the_survey_completed Monitoring_Program Date
        Temperature_end Sky_end Wind_end'''.split()

    COUNT_COLUMNS = '''
        event_id sci_name count A B C D E A_key B_key C_key D_key E_key
        Observer_Spotter Other_participants Recorder_Scribe
        Taxon_as_reported'''.split()

    def __init__(self, db):
        """Setup."""
        self.db = db
        self.cxn = self.db(dataset_id=self.DATASET_ID)
        self.bbs_cxn = None

    def ingest(self):
        """Ingest the data."""
        self.cxn.bulk_add_setup()
        self.cxn.delete_dataset()

        raw_places = self._get_raw_places()
        raw_data = self._get_raw_data()

        self._insert_dataset()
        to_place_id = self._insert_places(raw_places)
        to_taxon_id = self._insert_taxons(raw_data)
        raw_data = self._insert_events(raw_data, to_place_id)
        self._insert_counts(raw_data, to_taxon_id)

        self.cxn.update_places()
        self.cxn.bulk_add_cleanup()

    def _get_raw_places(self):
        print(f'Getting {self.DATASET_ID} raw place data')

        raw_places = pd.read_csv(
            self.POLLARD_PATH / 'Pollard_locations.csv', dtype='unicode')
        raw_places = raw_places.rename(columns=self.PLACE_RENAMES)
        raw_places['radius'] = None
        raw_places = raw_places.drop_duplicates(self.PLACE_KEYS)

        return raw_places

    def _get_raw_data(self):
        print(f'Getting {self.DATASET_ID} raw event and count data')

        raw_data = pd.read_csv(
            self.POLLARD_PATH / 'pollardbase_example_201802.csv',
            dtype='unicode')
        raw_data = raw_data.rename(columns=self.RAW_DATA_RENAMES)
        raw_data['Start_time'] = pd.to_datetime(
            raw_data['Start_time'], errors='coerce')

        raw_data = raw_data[
            raw_data['Start_time'].notna() & raw_data.sci_name.notna()]

        raw_data.sci_name = raw_data.sci_name.str.split().str.join(' ')
        raw_data['genus'] = raw_data.sci_name.str.split().str[0]

        return raw_data

    def _insert_taxons(self, raw_data):
        print(f'Inserting {self.DATASET_ID} taxons')
        taxons = raw_data.loc[:, ['sci_name', 'common_name', 'genus']]

        taxons['dataset_id'] = self.DATASET_ID
        taxons['class'] = 'lepidoptera'
        taxons['ordr'] = ''
        taxons['family'] = ''
        taxons['target'] = 't'
        # taxons = data.add_taxon_genera_records(taxons)
        taxons = taxons.drop_duplicates('sci_name')

        taxons = self.cxn.add_taxon_id(taxons)
        self.cxn.insert_taxons(taxons)

        return taxons.reset_index().set_index(
            'sci_name').taxon_id.to_dict()

    def _insert_places(self, raw_places):
        print(f'Inserting {self.DATASET_ID} places')
        places = raw_places.loc[:, self.PLACE_COLUMNS]

        places.lat = pd.to_numeric(places.lat, errors='coerce')
        places.lng = pd.to_numeric(places.lng, errors='coerce')
        places = places[places.lat.notna() & places.lng.notna()]

        places['dataset_id'] = self.DATASET_ID
        places['radius'] = None

        places = self.cxn.add_place_id(places)
        self.cxn.insert_places(places)

        return places.reset_index().set_index(
            self.PLACE_KEYS, verify_integrity=True).place_id.to_dict()

    def _insert_events(self, raw_data, to_place_id):
        print(f'Inserting {self.DATASET_ID} events')
        events = raw_data.loc[:, self.EVENT_COLUMNS]

        events['started'] = events['Start time'].dt.strftime('%H:%M:%S')
        events['ended'] = pd.to_datetime(
            events['End time'], format='%H:%M:%S', errors='coerce')
        events['year'] = events['Start time'].dt.strftime('%Y')
        events['day'] = events['Start time'].dt.strftime('%j')

        events['place_key'] = self._get_place_keys(events)
        events['place_id'] = events.place_key.map(to_place_id)

        has_place_id = events.place_id.notna()
        events = events[has_place_id]
        raw_data = raw_data[has_place_id].copy()

        events = events.drop(['place_key'] + self.PLACE_KEYS, axis=1)

        events = self.cxn.add_event_id(events)
        raw_data['event_id'] = events.index.values

        self.cxn.insert_events(events)

        return raw_data

    def _insert_counts(self, raw_data, to_taxon_id):
        print(f'Inserting {self.DATASET_ID} counts')
        counts = raw_data.loc[:, self.COUNT_COLUMNS].reset_index()
        counts['taxon_id'] = counts.sci_name.map(to_taxon_id)
        counts['count'] = counts['count'].fillna(0)
        counts.taxon_id = counts.taxon_id.astype(int)
        counts = self.cxn.add_count_id(counts)
        self.cxn.insert_counts(counts)

    def _get_place_keys(self, df):
        return tuple(zip(df.Site, df.Route))

    def _insert_dataset(self):
        print(f'Inserting {self.DATASET_ID} dataset')
        dataset = pd.DataFrame([dict(
            dataset_id=self.DATASET_ID,
            title='Pollard lepidoptera observations',
            extracted=str(date.today()),
            version='2018-02',
            url='')])
        dataset.set_index('dataset_id').to_sql(
            'datasets', self.cxn.engine, if_exists='append')