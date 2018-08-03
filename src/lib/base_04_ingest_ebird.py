"""Ingest eBird data."""

from datetime import date
import pandas as pd
import lib.sqlite as db
import lib.data as data


class BaseIngestEbird:
    """Ingest eBird data."""

    DATASET_ID = 'ebird'
    EBIRD_PATH = db.DATA_DIR / 'raw' / DATASET_ID
    EBIRD_CSV = EBIRD_PATH / 'ebd_relFeb-2018.txt'

    PLACE_KEY_COLUMNS = ['lng', 'lat']
    PLACE_COLUMNS = db.PLACE_COLUMNS + [
        'COUNTRY CODE', 'STATE CODE', 'COUNTY CODE', 'IBA CODE', 'BCR CODE',
        'USFWS CODE', 'ATLAS BLOCK', 'LOCALITY ID', ' LOCALITY TYPE',
        'EFFORT AREA HA']

    EVENT_KEY_COLUMN = 'SAMPLING EVENT IDENTIFIER'
    EVENT_COLUMNS = db.EVENT_COLUMNS + PLACE_KEY_COLUMNS + [
        'EFFORT AREA HA', 'APPROVED', 'REVIEWED', 'NUMBER OBSERVERS',
        'ALL SPECIES REPORTED', 'OBSERVATION DATE', 'GROUP IDENTIFIER']

    COUNT_COLUMNS = db.COUNT_COLUMNS + EVENT_KEY_COLUMN + [
        'GLOBAL UNIQUE IDENTIFIER', 'LAST EDITED DATE', 'TAXONOMIC ORDER',
        'CATEGORY', 'SUBSPECIES SCIENTIFIC NAME', 'BREEDING BIRD ATLAS CODE',
        'BREEDING BIRD ATLAS CATEGORY', 'AGE/SEX', 'OBSERVER ID', 'HAS MEDIA']

    def __init__(self, db):
        """Setup."""
        self.db = db
        self.cxn = self.db(dataset_id=self.DATASET_ID)
        self.bbs_cxn = None

    def ingest(self):
        """Ingest the data."""
        self.cxn.bulk_add_setup()
        self.cxn.delete_dataset()

        to_taxon_id = self._get_raw_taxons()
        self._insert_dataset()
        self._insert_codes()

        chunk = 1_000_000
        reader = pd.read_csv(
            self.EBIRD_CSV,
            delimiter='\t',
            quoting=3,
            chunksize=chunk,
            dtype='unicode')

        to_place_id = {}
        to_event_id = {}

        for i, raw_data in enumerate(reader, 1):
            print(f'Processing {self.DATASET_ID} chunk {i * chunk:,}')

            raw_data = self._filter_data(raw_data, to_taxon_id)

            if raw_data.shape[0] == 0:
                continue

            to_place_id = self._insert_places(raw_data, to_place_id)
            to_event_id = self._insert_events(
                raw_data, to_place_id, to_event_id)
            self._insert_counts(raw_data, to_event_id, to_taxon_id)

        self.cxn.update_places()
        self.cxn.bulk_add_cleanup()

    def _get_raw_taxons(self):
        """Build a dictionary of scientific names and taxon_ids."""
        print(f'Getting {self.DATASET_ID} raw taxon data')
        sql = """SELECT taxon_id, sci_name FROM taxons WHERE target = 't'"""
        taxons = pd.read_sql(sql, self.cxn.engine).set_index('sci_name')

        return taxons.set_index('sci_name').taxon_id.to_dict()

    def _filter_data(self, raw_data, to_taxon_id):
        raw_data = raw_data.rename(columns={
            'LATITUDE': 'lat',
            'LONGITUDE': 'lng',
            'EFFORT DISTANCE KM': 'radius',
            'TIME OBSERVATIONS STARTED': 'started',
            'OBSERVATION COUNT': 'count'})

        raw_data['event_date'] = pd.to_datetime(
            raw_data['OBSERVATION DATE'], errors='coerce')
        has_date = raw_data.event_date.notna()
        has_count = pd.to_numeric(raw_data['count'], errors='coerce').notna()
        is_approved = raw_data.APPROVED == '1'
        is_complete = raw_data['ALL SPECIES REPORTED'] == '1'
        in_species = raw_data['SCIENTIFIC NAME'].isin(to_taxon_id)
        raw_data = raw_data.loc[
            has_date & has_count & is_approved & is_complete & in_species, :]
        raw_data = data.filter_lat_lng(
            raw_data, lat=(20.0, 90.0), lng=(-95.0, -50.0))
        return raw_data

    def _insert_places(self, raw_data, to_place_id):
        print(f'Inserting {self.DATASET_ID} places')
        places = raw_data[self.PLACE_COLUMNS]

        places['place_key'] = self._get_place_keys(places)
        dups = places.place_key.duplicated()
        places = places[~dups]

        old_places = places.place_key.isin(to_place_id)
        places = places[~old_places]

        is_na = places.radius.isna()
        places.radius = pd.to_numeric(
            places.radius, errors='coerce').fillna(0.0)
        places.radius *= 1000.0
        places.loc[is_na, 'radius'] = None

        places['dataset_id'] = self.DATASET_ID
        places = self.cxn.add_place_id(places)

        new_place_ids = places.set_index('place_key').place_id.to_dict()
        to_place_id = {**to_place_id, **new_place_ids}

        places = places.drop(['place_key'], axis=1)
        self.cxn.insert_places(places)

        return to_place_id

    def _insert_events(self, raw_data, to_place_id, to_event_id):
        print(f'Inserting {self.DATASET_ID} events')
        events = raw_data[self.EVENT_COLUMNS]

        events['place_key'] = self._get_place_keys(events)

        dups = events[self.EVENT_KEY_COLUMN].duplicated()
        events = events[~dups]

        old_events = events[self.EVENT_KEY_COLUMN].isin(to_event_id)
        events = events[~old_events]

        events['place_id'] = events.place_key.map(to_place_id)
        events['year'] = events.event_date.dt.strftime('%Y')
        events['day'] = events.event_date.dt.strftime('%j')

        events['started'] = pd.to_datetime(
            events['started'], format='%H:%M:%S')
        events['delta'] = pd.to_numeric(
            events['DURATION MINUTES'], errors='coerce')
        events.delta = pd.to_timedelta(events.delta, unit='m', errors='coerce')
        events['ended'] = events.started + events.delta
        self._convert_to_time(events, 'started')
        self._convert_to_time(events, 'ended')

        new_event_ids = events.set_index(
            self.EVENT_KEY_COLUMN).event_id.to_dict()
        to_event_id = {**to_event_id, **new_event_ids}

        events = events.drop(['place_key'] + self.PLACE_KEY_COLUMNS, axis=1)
        self.cxn.insert_events(events)

        return to_event_id

    def _insert_counts(self, raw_data, to_event_id, to_taxon_id):
        print(f'Inserting {self.DATASET_ID} counts')
        counts = raw_data[self.COUNT_COLUMNS]

        counts['taxon_id'] = counts['SCIENTIFIC NAME'].map(to_taxon_id)
        counts['event_id'] = counts[self.EVENT_KEY_COLUMN].map(to_event_id)

        counts.count = counts.count.apply(int)
        counts.event_id = counts.event_id.astype(int)
        counts.taxon_id = counts.taxon_id.astype(int)

        counts = self.cxn.add_count_id(counts)
        counts = counts.drop([self.EVENT_KEY_COLUMN], axis=1)
        self.cxn.insert_counts(counts)

    def _get_place_keys(self, df):
        return tuple(zip(df.lng, df.lat))

    @staticmethod
    def _convert_to_time(df, column):
        """Convert the time field from datetime format to HH:MM format."""
        is_na = df[column].isna()
        df[column] = df[column].dt.strftime('%H:%M')
        df.loc[is_na, column] = None

    def _insert_dataset(self):
        print(f'Inserting {self.DATASET_ID} dataset')
        dataset = pd.DataFrame([dict(
            dataset_id=self.DATASET_ID,
            title='eBird Basic Dataset',
            extracted=str(date.today()),
            version='relFeb-2018',
            url='https://ebird.org/home')])
        dataset.set_index('dataset_id').to_sql(
            'datasets', self.cxn.engine, if_exists='append')

    def _insert_codes(self):
        print(f'Inserting {self.DATASET_ID} codes')

        bcr = pd.read_csv(
            self.EBIRD_PATH / 'BCRCodes.txt', sep='\t', encoding='ISO-8859-1')
        bcr['field'] = 'BCR CODE'

        iba = pd.read_csv(
            self.EBIRD_PATH / 'IBACodes.txt', sep='\t', encoding='ISO-8859-1')
        iba['field'] = 'IBA CODE'

        usfws = pd.read_csv(
            self.EBIRD_PATH / 'USFWSCodes.txt',
            sep='\t',
            encoding='ISO-8859-1')
        usfws['field'] = 'USFWS CODE'

        codes = pd.read_csv(self.EBIRD_PATH / 'ebird_codes.csv')
        codes = codes.append([bcr, iba, usfws], ignore_index=True, sort=True)

        codes.to_sql('maps_codes', self.cxn.engine, if_exists='replace')
