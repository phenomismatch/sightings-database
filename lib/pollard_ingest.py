"""Ingest Pollard data."""

from datetime import date
import pandas as pd
import lib.util as util


class PollardIngest:
    """Ingest Pollard data."""

    DATASET_ID = util.POLLARD_DATASET_ID
    PLACE_KEYS = ['Site', 'Route']

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
        to_taxon_id = self._select_taxons()

        self._insert_dataset()
        to_place_id = self._insert_places(raw_places, raw_data)
        raw_data = self._insert_events(raw_data, to_place_id)
        self._insert_counts(raw_data, to_taxon_id)

        self.cxn.update_places()
        self.cxn.bulk_add_cleanup()

    def _get_raw_places(self):
        print(f'Getting {self.DATASET_ID} raw place data')

        place_renames = {'long': 'lng', 'Land Owner': 'Land_Owner'}
        raw_places = pd.read_csv(
            util.POLLARD_PATH / 'Pollard_locations.csv', dtype='unicode')
        raw_places = raw_places.rename(columns=place_renames)

        raw_places['radius'] = None
        raw_places = raw_places.drop_duplicates(self.PLACE_KEYS)

        return raw_places

    def _get_raw_data(self):
        print(f'Getting {self.DATASET_ID} raw event and count data')

        raw_data = pd.read_csv(
            util.POLLARD_PATH / 'pollardbase_example_201802.csv',
            dtype='unicode')

        raw_data = raw_data.rename(columns={
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
            'Observer/Spotter': 'Observer_Spotter',
            'Other participants': 'Other_participants',
            'Recorder/Scribe': 'Recorder_Scribe',
            'Taxon as reported': 'Taxon_as_reported',
            'Total': 'count'})

        raw_data['started'] = pd.to_datetime(
            raw_data['Start_time'], errors='coerce')
        raw_data['ended'] = pd.to_datetime(
            raw_data['End_time'], errors='coerce')

        raw_data.sci_name = raw_data.sci_name.str.split().str.join(' ')
        raw_data['genus'] = raw_data.sci_name.str.split().str[0]

        raw_data = raw_data[
            raw_data.started.notna() & raw_data.sci_name.notna()]

        return raw_data

    def _select_taxons(self):
        sql = """
            SELECT sci_name, taxon_id
              FROM taxons
             WHERE "class" = 'lepidoptera'
               AND target = 't'
            """
        taxons = pd.read_sql(sql, self.cxn.engine)
        return taxons.set_index('sci_name').taxon_id.to_dict()

    def _insert_places(self, raw_places, raw_data):
        print(f'Inserting {self.DATASET_ID} places')

        place_columns = '''
            lat lng Site Route County State Land_Owner transect_id Route_Poin
            Route_Po_1 Route_Po_2 CLIMDIV_ID CD_sub CD_Name ST PRE_MEAN PRE_STD
            TMP_MEAN TMP_STD'''.split()

        place_df = raw_data.drop_duplicates(['Site', 'Route'])
        columns = [c for c in place_columns if c in place_df.columns]
        place_df = place_df.loc[:, columns].copy()

        places = pd.merge(
            raw_places, place_df, how='left', on=['Site', 'Route'])

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

        event_columns = '''
            Site Route County State Start_time End_time Duration Survey Temp
            Sky Wind Archived Was_the_survey_completed Monitoring_Program Date
            Temperature_end Sky_end Wind_end started ended'''.split()
        events = raw_data.loc[:, event_columns].copy()

        events['year'] = events['started'].dt.strftime('%Y')
        events['day'] = events['started'].dt.strftime('%j')
        events['started'] = events['started'].dt.strftime('%H:%M:%S')
        events['ended'] = events['ended'].dt.strftime('%H:%M:%S')

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

        count_columns = '''
            event_id sci_name count A B C D E A_key B_key C_key D_key E_key
            Observer_Spotter Other_participants Recorder_Scribe
            Taxon_as_reported'''.split()
        counts = raw_data.loc[:, count_columns].copy().reset_index()

        counts['taxon_id'] = counts.sci_name.map(to_taxon_id)
        counts['count'] = counts['count'].fillna(0)
        counts.taxon_id = counts.taxon_id.astype(int)
        counts = self.cxn.add_count_id(counts)
        counts = counts.drop(['sci_name'], axis='columns')
        self.cxn.insert_counts(counts)

    def _get_place_keys(self, df):
        return tuple(zip(df.Site, df.Route))

    def _insert_dataset(self):
        print(f'Inserting {self.DATASET_ID} dataset')
        dataset = pd.DataFrame([{
            'dataset_id': self.DATASET_ID,
            'title': 'Pollard lepidoptera observations',
            'extracted': str(date.today()),
            'version': '2018-02',
            'url': ''}])
        dataset.set_index('dataset_id').to_sql(
            'datasets', self.cxn.engine, if_exists='append')


class PollardIngestPostgres(PollardIngest):
    """Ingest Pollard data into the Postgres database."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


class PollardIngestSqlite(PollardIngest):
    """Ingest Pollard data into the SQLite3 database."""
