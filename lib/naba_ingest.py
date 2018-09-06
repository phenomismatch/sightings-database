"""Ingest NABA data."""

from datetime import date
import pandas as pd
import lib.globals as g


class NabaIngest:
    """Ingest NABA data."""

    PLACE_KEYS = ['lng', 'lat']
    DATASET_ID = g.NABA_DATSET_ID

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
        to_taxon_id = self._select_taxons()

        self._insert_dataset()
        to_place_id = self._insert_places(raw_data)
        to_event_id = self._insert_events(raw_data, to_place_id)
        self._insert_counts(raw_data, to_event_id, to_taxon_id)

        self.cxn.update_places()
        self.cxn.bulk_add_cleanup()

    def _get_raw_data(self):
        print(f'Getting {self.DATASET_ID} raw data')

        raw_data = pd.read_csv(
            g.NABA_PATH / 'NABA_JULY4.csv', dtype='unicode')

        raw_data = raw_data.rename(columns={
            'Year': 'year',
            'Longitude': 'lng',
            'Latitude': 'lat',
            'Total_Count': 'count',
            'Gen/Tribe/Fam': 'genus',
            'Species_Epithet': 'species',
            'Lat_Long_Precision': 'radius'})

        raw_data.lat = pd.to_numeric(raw_data.lat, errors='coerce')
        raw_data.lng = pd.to_numeric(raw_data.lng, errors='coerce')
        raw_data.year = raw_data.year.astype(float).astype(int)
        raw_data.Month = raw_data.Month.astype(float).astype(int)
        raw_data.Day = raw_data.Day.astype(float).astype(int)
        raw_data['count'] = raw_data[
            'count'].fillna(0).astype(float).astype(int)
        raw_data['sci_name'] = raw_data.apply(
            lambda x: f'{x.genus} {x.species}', axis='columns')

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

    def _insert_places(self, raw_data):
        print(f'Inserting {self.DATASET_ID} places')

        place_columns = [
            '1D_Lat', '1D_Long', 'lat', 'lng', 'Lat_Long_Type', 'radius']
        places = raw_data.loc[:, place_columns]

        places = places[places.lat.notna() & places.lng.notna()]
        places = places.drop_duplicates(['lng', 'lat'])

        places.radius = pd.to_numeric(places.lat, errors='coerce')
        places['dataset_id'] = self.DATASET_ID

        places = self.cxn.add_place_id(places)
        self.cxn.insert_places(places)

        return places.reset_index().set_index(
            self.PLACE_KEYS, verify_integrity=True).place_id.to_dict()

    def _insert_events(self, raw_data, to_place_id):
        print(f'Inserting {self.DATASET_ID} events')

        event_columns = ['Program', 'year', 'Month', 'Day'] + self.PLACE_KEYS
        events = raw_data.loc[:, event_columns]
        events = events.drop_duplicates(['year', 'Month', 'Day'])

        events['date'] = events.apply(
            lambda x: f'{x.year}-{x.Month}-{x.Day}', axis=1)
        events['date'] = pd.to_datetime(events['date'])
        events['day'] = events['date'].dt.strftime('%j')

        events['started'] = None
        events['ended'] = None

        events['place_key'] = tuple(zip(events.lng, events.lat))
        events['place_id'] = events.place_key.map(to_place_id)

        events = events.drop(['place_key', 'date'] + self.PLACE_KEYS, axis=1)

        events = self.cxn.add_event_id(events)

        self.cxn.insert_events(events)

        return events.reset_index().set_index(
            ['year', 'Month', 'Day'], verify_integrity=True).event_id.to_dict()

    def _insert_counts(self, raw_data, to_event_id, to_taxon_id):
        print(f'Inserting {self.DATASET_ID} counts')

        raw_data['key'] = tuple(zip(
            raw_data.year, raw_data.Month, raw_data.Day))

        count_columns = '''
                MASTER_ID UMD_Species_Code count key sci_name'''.split()
        counts = raw_data.loc[:, count_columns].copy()

        counts['taxon_id'] = counts.sci_name.map(to_taxon_id)
        counts['event_id'] = counts.key.map(to_event_id)

        counts = counts.drop(['key', 'sci_name'], axis=1)
        counts = self.cxn.add_count_id(counts)
        self.cxn.insert_counts(counts)

    def _insert_dataset(self):
        print(f'Inserting {self.DATASET_ID} dataset')
        dataset = pd.DataFrame([{
            'dataset_id': self.DATASET_ID,
            'title': 'NABA',
            'extracted': str(date.today()),
            'version': '2018-07-04',
            'url': ''}])
        dataset.set_index('dataset_id').to_sql(
            'datasets', self.cxn.engine, if_exists='append')


class NabaIngestPostgres(NabaIngest):
    """Ingest NABA data."""

    def _insert_codes(self):
        super()._insert_codes()
        self.cxn.execute(
            f'ALTER TABLE {self.DATASET_ID}_codes ADD PRIMARY KEY (code_id)')


class NabaIngestSqlite(NabaIngest):
    """Ingest Pollard data into the SQLite3 database."""
