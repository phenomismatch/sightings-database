"""Functions for dealing with the sqlite database."""

from os import fspath
from pathlib import Path
import sqlite3


DATA_DIR = Path('data')
# DATA_DIR = Path('..') / 'data'
SQLITE_DB = fspath(DATA_DIR / 'processed' / 'macrosystems.sqlite.db')
SQLITE_PATH = fspath(Path('src') / 'sql' / 'sqlite_01_create_db.sql')
SPATIALITE_MODULE = '/usr/local/lib/mod_spatialite.so'

SQLITE_CREATE = f'spatialite {SQLITE_DB} < {SQLITE_PATH}'
# SQLITE_CREATE = f'sqlite3 {SQLITE_DB} < {SQLITE_PATH}'

EVENT_INDEX = 'event_id'
EVENT_COLUMNS = """dataset_id year day start_time end_time
                   latitude longitude radius geohash""".split()

COUNT_INDEX = 'count_id'
COUNT_COLUMNS = 'event_id taxon_id count'.split()


def connect(path=SQLITE_DB):
    """Connect to the database."""
    cxn = sqlite3.connect(path)

    cxn.execute("PRAGMA page_size = {}".format(2**16))
    cxn.execute("PRAGMA busy_timeout = 10000")
    cxn.execute("PRAGMA synchronous = OFF")
    cxn.execute("PRAGMA journal_mode = OFF")

    cxn.enable_load_extension(True)
    cxn.execute("SELECT load_extension('{}')".format(SPATIALITE_MODULE))

    return cxn


def exists(cxn, table):
    """Check if a table exists."""
    sql = """
        SELECT COUNT(*) AS count
          FROM sqlite_master
         WHERE type='table' AND name = ?"""
    results = cxn.execute(sql, (table, ))
    return results.fetchone()[0]


def next_id(cxn, table):
    """Get the max value from the table's field."""
    if not exists(cxn, table):
        return 1
    field = table[:-1] + '_id'
    sql = 'SELECT COALESCE(MAX({}), 0) AS id FROM {}'.format(field, table)
    return cxn.execute(sql).fetchone()[0] + 1


def last_insert_rowid(cxn):
    """
    Get the last inserted row ID.

    It's a global so we have to do it immediately after the insert.
    """
    return cxn.execute('SELECT last_insert_rowid() AS id').fetchone()[0]


def insert_dataset(cxn, rec):
    """Insert a dataset record."""
    sql = """
        INSERT INTO datasets (dataset_id, title, extracted, version, url)
             VALUES (:dataset_id, :title, :extracted, :version, :url)
        """
    cxn.execute(sql, rec)
    cxn.commit()
    return last_insert_rowid(cxn)


def select_dataset_points(cxn, dataset_id):
    """Select all points from a dataset."""
    sql = """
        SELECT event_id, longitude, latitude
          FROM events
         WHERE dataset_id = ?
        """
    result = cxn.execute(sql, (dataset_id, ))
    return result.fetchall()


def delete_dataset(cxn, dataset_id):
    """Clear MAPS data from the database."""
    print(f'Deleting old {dataset_id} records')

    cxn.execute("DELETE FROM datasets WHERE dataset_id = ?", (dataset_id, ))

    sql = """DELETE FROM events
            WHERE dataset_id NOT IN (SELECT dataset_id FROM datasets)"""
    cxn.execute(sql)

    sql = """DELETE FROM counts
              WHERE event_id NOT IN (SELECT event_id FROM events)"""
    cxn.execute(sql)

    cxn.commit()

    for sidecar in ['codes', 'counts', 'events']:
        cxn.execute(f'DROP TABLE IF EXISTS {dataset_id}_{sidecar}')


def update_point_macro(event):
    """Return a macro for updating the given point."""
    return """
        UPDATE events
           SET point = GeomFromText('POINT({} {})', 4326)
         WHERE event_id = {};
        """.format(event[1], event[2], event[0])


def update_points(cxn, dataset_id):
    """Update point records with the point geometry."""
    print('Updating points')

    points = []
    for i, event in enumerate(select_dataset_points(cxn, dataset_id), 1):
        points.append(update_point_macro(event))
        if i % 100_000 == 0:
            print(f'Completed {i:,}')
            script = ''.join(points)
            cxn.executescript(script)
            cxn.commit()
            points = []

    script = ''.join(points)
    cxn.executescript(script)
    cxn.commit()
