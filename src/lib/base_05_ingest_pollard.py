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
        '''A, B, C, D, E, A-key, B-key, C-key, D-key, E-key,
           Observer/Spotter, Other participants, Recorder/Scribe,
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
        raw_taxons = self._get_raw_taxons(raw_data)

        self._insert_dataset()
        self._insert_codes()
        to_taxon_id = self._insert_taxons(raw_taxons)
        to_place_id = self._insert_places(raw_places)
        to_event_id = self._insert_events(raw_data, to_place_id)
        self._insert_counts(raw_data, to_event_id, to_taxon_id)

        self.cxn.update_places()
        self.cxn.bulk_add_cleanup()

    def _get_raw_places(self):
        print(f'Getting {self.DATASET_ID} raw place data')
        raw_places = pd.read_csv(
            self.POLLARD_PATH / 'Pollard_places.csv', dtype='unicode')
        raw_places = raw_places.rename(columns={'long': 'lng'})
        raw_places = raw_places.drop_duplicates(['Site', 'Route'])
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
        return raw_data

    def _get_raw_taxons(self, raw_data):
        print(f'Getting {self.DATASET_ID} raw taxon data')

        parts = raw_data.sci_name.str.split(
            expand=True).drop([2], axis=1).rename(
            columns={0: 'genus', 1: 'combined'})
        raw_taxons = raw_data.join(parts)

        parts = raw_taxons.combined.str.split('/', expand=True).rename(
            columns={0: 'species', 1: 'synonym'})
        raw_taxons = raw_taxons.join(parts)

        raw_taxons = raw_taxons[raw_taxons.species.notna()]
        raw_taxons.sci_name = raw_taxons.genus + ' ' + raw_taxons.species

        has_synonym = raw_taxons.synonym.notna()
        raw_taxons.loc[has_synonym, 'synonyms'] = (
            raw_taxons.loc[has_synonym, 'genus']
            + ' '
            + raw_taxons.loc[has_synonym, 'synonym'])

        raw_taxons = raw_taxons.drop(
            ['synonym', 'species', 'combined'], axis=1)
        return raw_taxons

    def _insert_taxons(self, raw_taxons):
        print(f'Inserting {self.DATASET_ID} taxons')
        taxons = raw_taxons.loc[:, ['sci_name', 'common_name']]

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

    def _insert_places(self, raw_places):
        print(f'Inserting {self.DATASET_ID} places')
        places = raw_places.loc[:, self.PLACE_COLUMNS]
        return []

    def _insert_events(self, raw_data, to_place_id):
        print(f'Inserting {self.DATASET_ID} events')
        events = raw_data.loc[:, self.EVENT_COLUMNS]
        return []

    def _insert_counts(self, raw_data, to_event_id):
        print(f'Inserting {self.DATASET_ID} counts')
        counts = raw_data.loc[:, self.COUNT_COLUMNS]

#     pollard['Start time'] = pd.to_datetime(
#         pollard['Start time'], errors='coerce')
#   pollard = pollard[pollard['Start time'].notna() & pollard.sci_name.notna()]
#
#     pollard = pd.merge(pollard, places, on=['Site', 'Route'], how='left')
#     pollard.lat = pd.to_numeric(pollard.lat, errors='coerce')
#     pollard.lng = pd.to_numeric(pollard.lng, errors='coerce')
#     pollard = pollard[pollard.lat.notna() & pollard.lng.notna()]
#
#     return pollard
#
#
#
# def _insert_records(cxn, pollard, taxons):
#     print('Inserting event and count records')
#
#     pollard['dataset_id'] = DATASET_ID
#     pollard['started'] = pollard['Start time'].dt.strftime('%H:%M:%S')
#     pollard['ended'] = pd.to_datetime(
#         pollard['End time'], format='%H:%M:%S', errors='coerce')
#     pollard['year'] = pollard['Start time'].dt.strftime('%Y')
#     pollard['day'] = pollard['Start time'].dt.strftime('%j')
#     pollard['radius'] = None
#     pollard['geohash'] = pollard.apply(lambda x: geohash2.encode(
#         x.lat, x.lng, precision=7), axis=1)
#     pollard['taxon_id'] = pollard.sci_name.map(taxons)
#     event_id = db.next_id(cxn, 'events')
#     pollard['event_id'] = range(event_id, event_id + pollard.shape[0])
#     count_id = db.next_id(cxn, 'counts')
#     pollard['count_id'] = range(count_id, count_id + pollard.shape[0])
#
#
#     pollard = pollard.set_index('event_id')
#     data.insert_events(pollard.loc[:, event_cols], cxn, 'pollard_events')
#
#     pollard = pollard.reset_index().set_index('count_id')
#     data.insert_counts(pollard.loc[:, count_cols], cxn, 'pollard_counts')
