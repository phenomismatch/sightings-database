"""Common functions for dealing with database connections."""

from os import fspath, remove
from os.path import abspath, exists
from datetime import datetime
import sqlite3
import subprocess
from pathlib import Path
from lib.log import log


PROCESSED = Path('data') / 'processed'
INTERIM = Path('data') / 'interim'
DB_FILE = abspath(PROCESSED / 'sightings.sqlite.db')
SCRIPT_PATH = Path('sql')


TABLES = 'version datasets countries codes taxons places events counts'.split()
PLACE_FIELDS = 'place_id dataset_id lng lat radius place_json'.split()
EVENT_FIELDS = 'event_id place_id year day started ended event_json'.split()
COUNT_FIELDS = 'count_id event_id taxon_id count count_json'.split()


def connect(path=None):
    """Connect to an SQLite database."""
    path = path if path else str(DB_FILE)
    cxn = sqlite3.connect(path)

    cxn.execute('PRAGMA page_size = {}'.format(2**16))
    cxn.execute('PRAGMA busy_timeout = 10000')
    cxn.execute('PRAGMA synchronous = OFF')
    cxn.execute('PRAGMA journal_mode = OFF')
    return cxn


def aux_db(cxn, aux_path, aux_name='aux'):
    """Attach annother database to the current DB connection."""
    cxn.execute("ATTACH DATABASE '{aux_path}' AS {aux_name}")


def aux_detach(cxn, aux_name='aux'):
    """Detach the temporary database."""
    cxn.execute(f'DETACH DATABASE {aux_name}')


def create():
    """Create the database."""
    log(f'Creating database')

    script = fspath(SCRIPT_PATH / 'create_db_sqlite.sql')
    cmd = f'sqlite3 {DB_FILE} < {script}'

    if exists(DB_FILE):
        remove(DB_FILE)

    subprocess.check_call(cmd, shell=True)


def backup_database():
    """Backup the SQLite3 database."""
    log('Backing up SQLite3 database')
    now = datetime.now()
    backup = f'{DB_FILE[:-3]}_{now.strftime("%Y-%m-%d")}.db'
    cmd = f'cp {DB_FILE} {backup}'
    subprocess.check_call(cmd, shell=True)


def insert_version():
    """Insert the DB verion."""
    cxn = connect()
    sql = 'INSERT INTO version (version) VALUES (?)'
    cxn.execute(sql, ('v0.5',))
    cxn.commit()


def insert_dataset(dataset):
    """Insert the DB verion."""
    cxn = connect()
    sql = """INSERT INTO datasets (dataset_id, version, title, url)
                  VALUES (:dataset_id, :version, :title, :url)"""
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
    if not table_exists(cxn, table):
        return 1
    field = table[:-1] + '_id'
    sql = 'SELECT COALESCE(MAX({}), 0) AS id FROM {}'.format(field, table)
    return cxn.execute(sql).fetchone()[0] + 1


def table_exists(cxn, table):
    """Check if a table exists."""
    sql = """
        SELECT COUNT(*) AS n
          FROM sqlite_master
         WHERE "type" = 'table'
           AND name = ?"""
    results = cxn.execute(sql, (table, ))
    return results.fetchone()[0]


def export_to_csv_files():
    """Export the SQLite3 database to CSV files."""
    log('Exporting the SQLite3 database into CSV files.')

    for table in TABLES:
        log(f'Exporting {table}')
        csv_file = INTERIM / f'{table}.csv'
        cmd = f'sqlite3 -csv {DB_FILE} '
        cmd += f'"select * from {table};" > {csv_file}'
        subprocess.check_call(cmd, shell=True)


def create_postgres():
    """Create the PostgreSQL DB."""
    script = fspath(SCRIPT_PATH / 'create_db_postgres.sql')
    cmd = f'psql -d sightings -a -f {script}'
    subprocess.check_call(cmd, shell=True)


def load_postgres():
    """Load data into the PostgreSQL database from CSV files."""
    log('Importing into PostgreSQL database')
    script = fspath(SCRIPT_PATH / 'import_db_postgres.sql')
    cmd = f'psql -d sightings -a -f {script}'
    subprocess.check_call(cmd, shell=True)
