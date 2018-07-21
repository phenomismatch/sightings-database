"""Functions for dealing with the sqlite database."""

import os
import subprocess
from pathlib import Path
import sqlite3


class Connection:
    """sqlite functions."""

    DATA_DIR = Path('data')
    # DATA_DIR = Path('..') / 'data'
    SQLITE_DB = os.fspath(DATA_DIR / 'processed' / 'sightings.sqlite.db')
    SPATIALITE_MODULE = '/usr/local/lib/mod_spatialite.so'

    CREATE_SCRIPT = os.fspath(Path('src') / 'sql' / 'sqlite_01_create_db.sql')
    CREATE_CMD = f'spatialite {SQLITE_DB} < {CREATE_SCRIPT}'
    # CREATE_CMD = f'sqlite3 {SQLITE_DB} < {CREATE_SCRIPT}'

    EVENT_INDEX = 'date_id'
    EVENT_COLUMNS = """dataset_id year day started ended
                       lat lng radius geohash""".split()

    COUNT_INDEX = 'count_id'
    COUNT_COLUMNS = 'date_id taxon_id count'.split()

    @classmethod
    def create(cls):
        """Create the database."""
        print('Creating database')
        if os.path.exists(cls.SQLITE_DB):
            os.remove(cls.SQLITE_DB)

        subprocess.check_call(cls.CREATE_CMD, shell=True)

    def __init__(self, path=SQLITE_DB):
        """Connect to the database and initialize parameters."""
        self.cxn = sqlite3.connect(path)
        self.engine = self.cxn

        self.cxn.execute("PRAGMA page_size = {}".format(2**16))
        self.cxn.execute("PRAGMA busy_timeout = 10000")
        self.cxn.execute("PRAGMA synchronous = OFF")
        self.cxn.execute("PRAGMA journal_mode = OFF")

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

    # def last_insert_rowid(cxn):
    #     """
    #     Get the last inserted row ID.
    #
    #     It's a DB global so we have to do it immediately after the insert.
    #     """
    #     return cxn.execute('SELECT last_insert_rowid() AS id').fetchone()[0]
    #
    # def insert_dataset(cxn, rec):
    #     """Insert a dataset record."""
    #     sql = """
    #         INSERT INTO datasets (dataset_id, title, extracted, version, url)
    #              VALUES (:dataset_id, :title, :extracted, :version, :url)
    #         """
    #     cxn.execute(sql, rec)
    #     cxn.commit()
    #
    # def select_dataset_points(cxn, dataset_id):
    #     """Select all points from a dataset."""
    #     sql = """
    #         SELECT date_id, lng, lat
    #           FROM dates
    #          WHERE dataset_id = ?
    #         """
    #     result = cxn.execute(sql, (dataset_id, ))
    #     return result.fetchall()
    #
    # def delete_dataset(cxn, dataset_id):
    #     """Clear MAPS data from the database."""
    #     print(f'Deleting old {dataset_id} records')
    #
    #     cxn.execute("DELETE FROM datasets WHERE dataset_id = ?", (dataset_id, ))
    #
    #     cxn.execute("DELETE FROM taxons WHERE dataset_id = ?", (dataset_id, ))
    #
    #     sql = """DELETE FROM dates
    #             WHERE dataset_id NOT IN (SELECT dataset_id FROM datasets)"""
    #     cxn.execute(sql)
    #
    #     sql = """DELETE FROM counts
    #               WHERE date_id NOT IN (SELECT date_id FROM dates)"""
    #     cxn.execute(sql)
    #
    #     cxn.commit()
    #
    #     for sidecar in ['codes', 'counts', 'dates']:
    #         cxn.execute(f'DROP TABLE IF EXISTS {dataset_id}_{sidecar}')
    #
    # def update_point_macro(event):
    #     """Return a macro for updating the given point."""
    #     return """
    #         UPDATE dates
    #            SET geopoint = GeomFromText('POINT({} {})', 4326)
    #          WHERE date_id = {};
    #         """.format(event[1], event[2], event[0])
    #
    # def update_points(cxn, dataset_id):
    #     """Update point records with the point geometry."""
    #     print('Updating points')
    #
    #     points = []
    #     for i, event in enumerate(self.select_dataset_points(cxn, dataset_id), 1):
    #         points.append(self.update_point_macro(event))
    #         if i % 100_000 == 0:
    #             print(f'Completed {i:,}')
    #             script = ''.join(points)
    #             cxn.executescript(script)
    #             cxn.commit()
    #             points = []
    #
    #     script = ''.join(points)
    #     cxn.executescript(script)
    #     cxn.commit()
