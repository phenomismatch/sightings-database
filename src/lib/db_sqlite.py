"""Functions for dealing with the sqlite database connections."""

import os
import subprocess
from pathlib import Path
import sqlite3
from lib.db import Db


class DbSqlite(Db):
    """sqlite functions."""

    DATA_DIR = Path('data')
    # DATA_DIR = Path('..') / 'data'
    SQLITE_DB = os.fspath(DATA_DIR / 'processed' / 'sightings.sqlite.db')
    SPATIALITE_MODULE = '/usr/local/lib/mod_spatialite.so'

    CREATE_SCRIPT = os.fspath(Path('src') / 'sql' / 'create_db_sqlite.sql')
    CREATE_CMD = f'spatialite {SQLITE_DB} < {CREATE_SCRIPT}'
    # CREATE_CMD = f'sqlite3 {SQLITE_DB} < {CREATE_SCRIPT}'

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
            SELECT COUNT(*) AS n
              FROM sqlite_master
             WHERE "type" = 'table'
               AND name = ?"""
        results = self.cxn.execute(sql, (table, ))
        return results.fetchone()[0]

    def upload_table(self, df, table, columns):
        """Upload the dataframe into the database."""
        df.loc[:, columns].to_sql(table, self.engine, if_exists='append')

    def update_places(self):
        """Update point records with the point geometry."""
        print(f'Updating {self.dataset_id} place points')

        sql = """
            UPDATE places
               SET geopoint = MakePoint(lng, lat, 4326)
             WHERE dataset_id = ?"""
        self.execute(sql, (self.dataset_id, ))

        sql = """
            UPDATE places
               SET geohash = GeoHash(geopoint, 7)
             WHERE dataset_id = ?"""
        self.execute(sql, (self.dataset_id, ))
