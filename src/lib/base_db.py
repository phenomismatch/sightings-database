"""Common functions for dealing with database connections."""


class BaseDb:
    """Common database functions."""

    PLACE_INDEX = 'place_id'
    PLACE_COLUMNS = """dataset_id lng lat radius""".split()  # geohash geopoint

    EVENT_INDEX = 'event_id'
    EVENT_COLUMNS = """place_id year day started ended""".split()

    COUNT_INDEX = 'count_id'
    COUNT_COLUMNS = 'event_id taxon_id count'.split()

    def delete_dataset(self):
        """Clear dataset from the database."""
        print(f'Deleting old {self.dataset_id} records')

        self.execute(
            'DELETE FROM datasets WHERE dataset_id = ?', (self.dataset_id, ))

        sql = """DELETE FROM places
                WHERE dataset_id NOT IN (SELECT dataset_id FROM datasets)"""
        self.execute(sql)

        sql = """DELETE FROM events
                  WHERE place_id NOT IN (SELECT place_id FROM places)"""
        self.execute(sql)

        sql = """DELETE FROM counts
                  WHERE event_id NOT IN (SELECT event_id FROM events)"""
        self.execute(sql)

        for sidecar in ['codes', 'places', 'events', 'counts']:
            self.execute(f'DROP TABLE IF EXISTS {self.dataset_id}_{sidecar}')

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
        self.upload_table(places, 'places', self.PLACE_COLUMNS)
        self.upload_sidecar(places, 'places', self.PLACE_COLUMNS)

    def insert_events(self, events):
        """Insert the events into the database."""
        self.upload_table(events, 'events', self.EVENT_COLUMNS)
        self.upload_sidecar(events, 'events', self.EVENT_COLUMNS)

    def insert_counts(self, counts):
        """Insert the counts into the database."""
        self.upload_table(counts, 'counts', self.COUNT_COLUMNS)
        self.upload_sidecar(counts, 'counts', self.COUNT_COLUMNS)

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
