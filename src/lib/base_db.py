"""Common functions for dealing with database connections."""


class BaseDb:
    """Common database functions."""

    PLACE_INDEX = 'place_id'
    PLACE_COLUMNS = """dataset_id lng lat radius""".split()  # geohash geopoint

    EVENT_INDEX = 'event_id'
    EVENT_COLUMNS = """place_id year day started ended""".split()

    COUNT_INDEX = 'count_id'
    COUNT_COLUMNS = 'event_id taxon_id count'.split()

    def delete_dataset(self, dataset_id):
        """Clear dataset from the database."""
        print(f'Deleting old {dataset_id} records')

        self.execute(
            'DELETE FROM datasets WHERE dataset_id = ?', (dataset_id, ))

        for sidecar in ['codes', 'places', 'events', 'counts']:
            self.execute(f'DROP TABLE IF EXISTS {dataset_id}_{sidecar}')

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

    def insert_places(self, places):
        """Insert the events into the database."""
        places.loc[:, self.PLACE_COLUMNS].to_sql(
            'places', self.engine, if_exists='append')
        sidecar = f'{self.dataset_id}_places'
        self.insert_sidecar(
            places, sidecar, self.PLACE_COLUMNS, self.PLACE_INDEX)

    def insert_events(self, events):
        """Insert the events into the database."""
        events.loc[:, self.EVENT_COLUMNS].to_sql(
            'events', self.engine, if_exists='append')
        sidecar = f'{self.dataset_id}_events'
        self.insert_sidecar(
            events, sidecar, self.EVENT_COLUMNS, self.EVENT_INDEX)

    def insert_counts(self, counts):
        """Insert the counts into the database."""
        counts.loc[:, self.COUNT_COLUMNS].to_sql(
            'counts', self.engine, if_exists='append')
        sidecar = f'{self.dataset_id}_counts'
        self.insert_sidecar(
            counts, sidecar, self.COUNT_COLUMNS, self.COUNT_INDEX)

    def insert_sidecar(self, df, sidecar, exclude, index):
        """Insert the sidecar table into the database."""
        columns = [c for c in df.columns if c not in exclude + ['key']]
        df.loc[:, columns].to_sql(sidecar, self.engine, if_exists='append')

    def update_places(self):
        """Update point records with the point geometry."""
        print(f'Updating {self.dataset_id} place points')
        sql = """
            UPDATE places
               SET geopoint = ST_SetSRID(ST_MakePoint(lng, lat), 4326),
                   geohash  = ST_GeoHash(ST_MakePoint(lng, lat), 7)
             WHERE dataset_id = ?"""
        self.execute(sql, (self.dataset_id, ))

    def bulk_add_setup(self):
        """Prepare the database for bulk adds."""
        print('Dropping indexes and constraints')
        self.drop_indexes()

    def bulk_add_teardown(self):
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
