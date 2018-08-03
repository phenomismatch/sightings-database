"""Ingest eBird data."""

from datetime import date
import warnings
import pandas as pd
import lib.sqlite as db
import lib.data as data


class BaseIngestEbird:
    """Ingest eBird data."""

    DATASET_ID = 'ebird'
    EBIRD_PATH = db.DATA_DIR / 'raw' / DATASET_ID
    EBIRD_CSV = EBIRD_PATH / 'ebd_relFeb-2018.txt'

    PLACE_COLUMNS = db.PLACE_COLUMNS + [
        'COUNTRY CODE', 'STATE CODE', 'COUNTY CODE', 'IBA CODE', 'BCR CODE',
        'USFWS CODE', 'ATLAS BLOCK', 'LOCALITY ID', ' LOCALITY TYPE',
        'EFFORT AREA HA']
    EVENT_COLUMNS = db.EVENT_COLUMNS + [
        'EFFORT AREA HA', 'APPROVED', 'REVIEWED', 'NUMBER OBSERVERS',
        'ALL SPECIES REPORTED', 'OBSERVATION DATE', 'GROUP IDENTIFIER']
    COUNT_COLUMNS = db.COUNT_COLUMNS + [
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

        raw_data['key'] = tuple(zip(raw_data.lng, raw_data.lat))
        dups = raw_data.key.duplicated()
        places = raw_data[~dups]

        old_places = places.key.isin(to_place_id)
        places = places[~old_places]

        is_na = places.radius.isna()
        places.radius = pd.to_numeric(
            places.radius, errors='coerce').fillna(0.0)
        places.radius *= 1000.0
        places.loc[is_na, 'radius'] = None

        places['dataset_id'] = self.DATASET_ID
        places = self.cxn.add_place_id(places)

        new_place_ids = places.set_index('key').place_id.to_dict()
        to_place_id = {**to_place_id, **new_place_ids}
        return to_place_id

    def _insert_events(self, raw_data, to_place_id, to_event_id):
        print(f'Inserting {self.DATASET_ID} events')
#         dups = df['SAMPLING EVENT IDENTIFIER'].duplicated()
#         events = df[~dups]
#         old_events = events['SAMPLING EVENT IDENTIFIER'].isin(sample_ids)

#         df['year'] = df.event_date.dt.strftime('%Y')
#         df['day'] = df.event_date.dt.strftime('%j')

#         df['started'] = pd.to_datetime(df['started'], format='%H:%M:%S')
#         df['delta'] = pd.to_numeric(df['DURATION MINUTES'], errors='coerce')
#         df.delta = pd.to_timedelta(df.delta, unit='m', errors='coerce')
#         df['ended'] = df.started + df.delta
#         convert_to_time(df, 'started')
#         convert_to_time(df, 'ended')


#         events = events[~old_events]
    def _insert_counts(self, raw_data, to_event_id, to_taxon_id):
        print(f'Inserting {self.DATASET_ID} counts')

#         df['count'] = df['count'].apply(int)
#         df['taxon_id'] = df['SCIENTIFIC NAME'].map(taxons)



#         event_id = db.next_id(cxn, 'events')
#         events['event_id'] = range(event_id, event_id + events.shape[0])

#         events = events.set_index('SAMPLING EVENT IDENTIFIER', drop=False)
#         new_sample_ids = events.event_id.to_dict()
#         sample_ids = {**sample_ids, **new_sample_ids}

    @staticmethod
    def convert_to_time(df, column):
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
