"""Functions for dealing with the sqlite database connections."""

import os
import subprocess
from pathlib import Path
import sqlite3
from lib.base_db import BaseDb


class SqliteDb(BaseDb):
    """sqlite functions."""

    DATA_DIR = Path('data')
    # DATA_DIR = Path('..') / 'data'
    SQLITE_DB = os.fspath(DATA_DIR / 'processed' / 'sightings.sqlite.db')
    SPATIALITE_MODULE = '/usr/local/lib/mod_spatialite.so'

    CREATE_SCRIPT = os.fspath(Path('src') / 'sql' / 'sqlite_01_create_db.sql')
    CREATE_CMD = f'spatialite {SQLITE_DB} < {CREATE_SCRIPT}'
    # CREATE_CMD = f'sqlite3 {SQLITE_DB} < {CREATE_SCRIPT}'

    EVENT_INDEX = 'event_id'
    EVENT_COLUMNS = """dataset_id year day started ended
                       lat lng radius geohash""".split()

    COUNT_INDEX = 'count_id'
    COUNT_COLUMNS = 'event_id taxon_id count'.split()

    @classmethod
    def create(cls):
        """Create the database."""
        print('Creating database')
        if os.path.exists(cls.SQLITE_DB):
            os.remove(cls.SQLITE_DB)

        subprocess.check_call(cls.CREATE_CMD, shell=True)

    def __init__(self, path=SQLITE_DB, dataset_id=None):
        """Connect to the database and initialize parameters."""
        self.cxn = sqlite3.connect(path)
        self.engine = self.cxn
        self.dataset_id = dataset_id

        self.cxn.execute("PRAGMA page_size = {}".format(2**16))
        self.cxn.execute("PRAGMA busy_timeout = 10000")
        self.cxn.execute("PRAGMA synchronous = OFF")
        self.cxn.execute("PRAGMA journal_mode = OFF")
        # self.cxn.execute("PRAGMA foreign_keys = ON")

        self.cxn.enable_load_extension(True)
        self.cxn.execute(
            "SELECT load_extension('{}')".format(self.SPATIALITE_MODULE))

    def execute(self, sql, values=None):
        """Execute and commit the given query."""
        if values:
            self.cxn.execute(sql, values)
        else:
            self.cxn.execute(sql)
        self.cxn.commit()

    def next_id(self, table):
        """Get the max value from the table's field."""
        if not self.exists(table):
            return 1
        field = table[:-1] + '_id'
        sql = 'SELECT COALESCE(MAX({}), 0) AS id FROM {}'.format(field, table)
        return self.cxn.execute(sql).fetchone()[0] + 1

    def exists(self, table):
        """Check if a table exists."""
        sql = """
            SELECT COUNT(*) AS count
              FROM sqlite_master
             WHERE type='table' AND name = ?"""
        results = self.cxn.execute(sql, (table, ))
        return results.fetchone()[0]

    # def update_place_macro(event):
    #     """Return a macro for updating the given point."""
    #     return """
    #         UPDATE events
    #            SET geopoint = GeomFromText('POINT({} {})', 4326)
    #          WHERE event_id = {};
    #         """.format(event[1], event[2], event[0])
    #
    # def update_places(cxn, dataset_id):
    #     """Update point records with the point geometry."""
    #     print('Updating places')
    #
    #     places = []
    #     for i, event in enumerate(
    #           self.select_dataset_places(cxn, dataset_id), 1):
    #         places.append(self.upevent_place_macro(event))
    #         if i % 100_000 == 0:
    #             print(f'Completed {i:,}')
    #             script = ''.join(places)
    #             cxn.executescript(script)
    #             cxn.commit()
    #             places = []
    #
    #     script = ''.join(places)
    #     cxn.executescript(script)
    #     cxn.commit()
