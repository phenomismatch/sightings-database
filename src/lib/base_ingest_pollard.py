"""Ingest Pollard data."""

import re
from datetime import date
import pandas as pd
import lib.globals as g


class BaseIngestPollard:
    """Ingest the MAPS data."""

    DATASET_ID = 'pollard'
    POLLARD_PATH = g.DATA_DIR / 'raw' / DATASET_ID

    PLACE_KEYS = ['Site', 'Route']
    PLACE_COLUMNS = re.split(
        r',\s*',
        '''lat, lng, Site, Route, County, State, Land Owner,
           transect_id, Route_Poin, Route_Po_1, Route_Po_2, CLIMDIV_ID,
           CD_sub, CD_Name, ST, PRE_MEAN, PRE_STD, TMP_MEAN, TMP_STD''')

    EVENT_COLUMNS = re.split(
        r',\s*',
        '''Site, Route, County, State, Land Owner, Start time, End time,
           Duration, Survey, Temp, Sky, Wind, Archived,
           Was the survey completed?, Monitoring Program, Date,
           Temperature (end), Sky (end), Wind (end)''')

    COUNT_COLUMNS = re.split(
        r',\s*',
        '''event_id, sci_name, count, A, B, C, D, E, A-key, B-key, C-key,
           D-key, E-key, Observer/Spotter, Other participants, Recorder/Scribe,
           Taxon as reported''')

    def __init__(self, db):
        """Setup."""
        self.db = db
        self.cxn = self.db(dataset_id=self.DATASET_ID)
        self.bbs_cxn = None

    def ingest(self):
        """Ingest the data."""
        self.cxn.bulk_add_setup()
        self.cxn.delete_dataset()

        raw_data = self._get_raw_data()
        raw_places = self._get_raw_places()

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
        raw_places = raw_places.rename(columns={'long': 'lng'})
        raw_places = raw_places.drop_duplicates(self.PLACE_KEYS)

        return raw_places

    def _get_raw_data(self):
        print(f'Getting {self.DATASET_ID} raw event and count data')

        raw_data = pd.read_csv(
            self.POLLARD_PATH / 'pollardbase_example_201802.csv',
            dtype='unicode')
        raw_data = raw_data.rename(columns={
            'Scientific Name': 'sci_name',
            'Species': 'common_name',
            'Total': 'count'})
        raw_data['Start time'] = pd.to_datetime(
            raw_data['Start time'], errors='coerce')
        raw_data = raw_data[
            raw_data['Start time'].notna() & raw_data.sci_name.notna()]

        parts = raw_data.sci_name.str.split(
            expand=True).drop([2], axis=1).rename(
            columns={0: 'genus', 1: 'combined'})
        raw_data = raw_data.join(parts)

        parts = raw_data.combined.str.split('/', expand=True).rename(
            columns={0: 'species', 1: 'synonym'})
        raw_data = raw_data.join(parts)

        raw_data = raw_data[raw_data.species.notna()]
        raw_data.sci_name = raw_data.genus + ' ' + raw_data.species

        has_synonym = raw_data.synonym.notna()
        raw_data.loc[has_synonym, 'synonyms'] = (
            raw_data.loc[has_synonym, 'genus']
            + ' '
            + raw_data.loc[has_synonym, 'synonym'])

        raw_data = raw_data.drop(
            ['synonym', 'species', 'combined'], axis=1)
        return raw_data

    def _insert_taxons(self, raw_data):
        print(f'Inserting {self.DATASET_ID} taxons')
        taxons = raw_data.loc[:, ['sci_name', 'common_name']]

        taxons = taxons.drop_duplicates('sci_name')
        taxons['dataset_id'] = self.DATASET_ID
        taxons['synonyms'] = ''
        taxons['class'] = 'lepidoptera'
        taxons['ordr'] = ''
        taxons['family'] = ''
        taxons['target'] = 1

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
