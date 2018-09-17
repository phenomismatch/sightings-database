"""Common functions for dealing with database connections."""


class Db:
    """Common database functions."""

    TAXON_ID = 'taxon_id'
    TAXON_COLUMNS = """taxon_dataset_id sci_name class order family genus
                       common_name""".split()

    CODE_COLUMNS = 'dataset_id field code value'.split()

    PLACE_INDEX = 'place_id'
    PLACE_COLUMNS = 'dataset_id lng lat radius'.split()  # geohash geopoint

    EVENT_INDEX = 'event_id'
    EVENT_COLUMNS = 'place_id year day started ended'.split()

    COUNT_INDEX = 'count_id'
    COUNT_COLUMNS = 'event_id taxon_id count'.split()

    def delete_dataset(self):
        """Clear dataset from the database."""
        print(f'Deleting old {self.dataset_id} records')

        self.execute(
            'DELETE FROM datasets WHERE dataset_id = ?', (self.dataset_id, ))

        self.execute(
            """DELETE FROM places
                WHERE dataset_id NOT IN (SELECT dataset_id FROM datasets)""")

        self.execute(
            """DELETE FROM events
                WHERE place_id NOT IN (SELECT place_id FROM places)""")

        self.execute(
            """DELETE FROM counts
                WHERE event_id NOT IN (SELECT event_id FROM "events")""")

        self.execute(
            """DELETE FROM counts
                WHERE taxon_id NOT IN (SELECT taxon_id FROM taxons)""")

        self.execute(
            """DELETE FROM codes
                WHERE dataset_id NOT IN (SELECT dataset_id FROM datasets)""")

    def add_taxon_id(self, taxons):
        """Add event IDs to the dataframe."""
        taxon_id = self.next_id('taxons')
        taxons['taxon_id'] = range(taxon_id, taxon_id + taxons.shape[0])
        return taxons.set_index('taxon_id')

    def add_place_id(self, places):
        """Add event IDs to the dataframe."""
        place_id = self.next_id('places')
        places['place_id'] = range(place_id, place_id + places.shape[0])
        return places.set_index('place_id')

    def add_event_id(self, events):
        """Add event IDs to the dataframe."""
        event_id = self.next_id('events')
        events['event_id'] = range(event_id, event_id + events.shape[0])
        return events.set_index('event_id')

    def add_count_id(self, counts):
        """Add count IDs to the dataframe."""
        count_id = self.next_id('counts')
        counts['count_id'] = range(count_id, count_id + counts.shape[0])
        return counts.set_index('count_id')

    def add_code_id(self, codes):
        """Add count IDs to the dataframe."""
        codes['code_id'] = range(codes.shape[0])
        return codes.set_index('code_id')

    def insert_taxons(self, taxons):
        """Insert taxons into the database."""
        self.upload_table(taxons, 'taxons', self.TAXON_COLUMNS)

    def insert_codes(self, codes):
        """Insert codes into the database."""
        self.upload_table(codes, 'codes', self.CODE_COLUMNS)

    def insert_places(self, places):
        """Insert places into the database."""
        self.add_json_data(places, 'place_json', self.PLACE_COLUMNS)
        columns = self.PLACE_COLUMNS + ['place_json']
        self.upload_table(places, 'places', columns)

    def insert_events(self, events):
        """Insert events into the database."""
        self.add_json_data(events, 'event_json', self.EVENT_COLUMNS)
        columns = self.EVENT_COLUMNS + ['event_json']
        self.upload_table(events, 'events', columns)

    def insert_counts(self, counts):
        """Insert counts into the database."""
        self.add_json_data(counts, 'count_json', self.COUNT_COLUMNS)
        columns = self.COUNT_COLUMNS + ['count_json']
        self.upload_table(counts, 'counts', columns)

    def add_json_data(self, df, json_column, columns):
        """Create a json field for all of the extra columns."""
        df['dataset_id'] = self.dataset_id
        columns = [c for c in df.columns
                   if c not in columns or c == 'dataset_id']
        df[json_column] = df.loc[:, columns].apply(
            lambda x: x.to_json(), axis='columns')

    def bulk_add_setup(self):
        """Prepare the database for bulk adds."""
        print('Dropping indexes and constraints')
        self.drop_indexes()

    def bulk_add_cleanup(self):
        """Prepare the database for use."""
        print('Adding indexes and constraints')
        self.add_indexes()

    def drop_indexes(self):
        """Drop indexes to speed up bulk data adds."""
        self.execute(f'DROP INDEX IF EXISTS places_lng_lat')
        self.execute(f'DROP INDEX IF EXISTS places_geohash')
        self.execute(f'DROP INDEX IF EXISTS events_year_day')

    def add_indexes(self):
        """Add indexes in bulk."""
        self.execute('CREATE INDEX places_lng_lat  ON places (lng, lat)')
        self.execute('CREATE INDEX places_geohash  ON places (geohash)')
        self.execute('CREATE INDEX events_year_day ON events (year, day)')
