"""Common functions for dealing with database connections."""

import os
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from lib.log import log


PROCESSED = Path('data') / 'processed'
DB_PATH = PROCESSED / 'sightings.sqlite.db'
SCRIPT_PATH = Path('lib') / 'sql' / 'sqlite'


def connect(path=None):
    """Connect to an SQLite database."""
    path = path if path else str(DB_PATH)
    cxn = sqlite3.connect(path)

    cxn.execute("PRAGMA page_size = {}".format(2**16))
    cxn.execute("PRAGMA busy_timeout = 10000")
    cxn.execute("PRAGMA synchronous = OFF")
    cxn.execute("PRAGMA journal_mode = OFF")
    return cxn


def create():
    """Create the database."""
    log(f'Creating database')

    script = os.fspath(SCRIPT_PATH / 'create_db.sql')
    cmd = f'sqlite3 {DB_PATH} < {script}'

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    subprocess.check_call(cmd, shell=True)


def bulk_add_setup():
    """Delete indices for faster inserts."""
    log(f'Dropping indices')

    script = os.fspath(SCRIPT_PATH / 'bulk_add_setup.sql')
    cmd = f'sqlite3 {DB_PATH} < {script}'

    subprocess.check_call(cmd, shell=True)


def bulk_add_cleanup():
    """Re-add indices for faster searches."""
    log(f'Recreating indices')

    script = os.fspath(SCRIPT_PATH / 'bulk_add_cleanup.sql')
    cmd = f'sqlite3 {DB_PATH} < {script}'

    subprocess.check_call(cmd, shell=True)


def insert_version():
    """Insert the DB verion."""
    cxn = connect()
    sql = 'INSERT INTO version (version, created) VALUES (?, ?)'
    cxn.execute(sql, ('v0.5',  datetime.now()))
    cxn.commit()


def insert_dataset(dataset):
    """Insert the DB verion."""
    cxn = connect()
    sql = """INSERT INTO datasets (dataset_id, extracted, version, title, url)
                  VALUES (:dataset_id, :extracted, :version, :title, :url)"""
    cxn.execute(sql, dataset)
    cxn.commit()


def delete_dataset(dataset_id):
    """Clear dataset from the database."""
    log(f'Deleting old {dataset_id} records')

    cxn = connect()
    cxn.execute('DELETE FROM datasets WHERE dataset_id = ?', (dataset_id, ))
    cxn.execute(
        """DELETE FROM taxons
            WHERE authority NOT IN (SELECT dataset_id FROM datasets)""")
    cxn.execute(
        """DELETE FROM places
            WHERE dataset_id NOT IN (SELECT dataset_id FROM datasets)""")
    cxn.execute(
        """DELETE FROM events
            WHERE place_id NOT IN (SELECT place_id FROM places)""")
    cxn.execute(
        """DELETE FROM counts
            WHERE event_id NOT IN (SELECT event_id FROM "events")""")
    cxn.execute(
        """DELETE FROM counts
            WHERE taxon_id NOT IN (SELECT taxon_id FROM taxons)""")
    cxn.execute(
        """DELETE FROM codes
            WHERE dataset_id NOT IN (SELECT dataset_id FROM datasets)""")
    cxn.commit()


def get_ids(df, table):
    """Get IDs to add to the dataframe."""
    start = next_id(table)
    return range(start, start + df.shape[0])


def next_id(table):
    """Get the max value from the table's ID field."""
    cxn = connect()
    if not exists(cxn, table):
        return 1
    field = table[:-1] + '_id'
    sql = 'SELECT COALESCE(MAX({}), 0) AS id FROM {}'.format(field, table)
    return cxn.execute(sql).fetchone()[0] + 1


def exists(cxn, table):
    """Check if a table exists."""
    sql = """
        SELECT COUNT(*) AS n
          FROM sqlite_master
         WHERE "type" = 'table'
           AND name = ?"""
    results = cxn.execute(sql, (table, ))
    return results.fetchone()[0]