"""Functions for dealing with the postgres database connections."""

import os
import re
import tempfile
import subprocess
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine
import lib.globals as g
from lib.db import Db

FILE_MODE = 644
ENGINE = 'postgresql://{}@localhost:5432/sightings'
CONNECT = 'dbname=sightings user={}'
CREATE_SCRIPT = os.fspath(Path('lib') / 'sql' / 'create_db_postgres.sql')
CREATE_CMD = f'psql -U postgres -d sightings -a -f {CREATE_SCRIPT}'


class DbPostgres(Db):
    """Postgresql connection."""

    @classmethod
    def create(cls):
        """Create the database."""
        print('Creating database')
        subprocess.check_call(CREATE_CMD, shell=True)

    def __init__(self, user='postgres', dataset_id=None):
        """Connect to the database and initialize parameters."""
        self.cxn = psycopg2.connect(CONNECT.format(user))
        self.engine = create_engine(ENGINE.format(user))
        self.dataset_id = dataset_id

    def execute(self, sql, values=None):
        """Execute and commit the given query."""
        sql = re.sub(r'\?', '%s', sql)
        self.cxn.cursor().execute(sql, values)
        self.cxn.commit()

    def next_id(self, table):
        """Get the max value from the table's field."""
        field = table[:-1] + '_id'
        sql = 'SELECT COALESCE(MAX({}), 0) AS id FROM {}'.format(field, table)
        with self.cxn.cursor() as cur:
            cur.execute(sql)
            max_id = cur.fetchone()[0]
        return max_id + 1

    def upload_table(self, df, table, columns):
        """Upload the dataframe into the database."""
        df = df.loc[:, columns]
        fd, path = self.create_csv(df)
        self.copy_table(df, table, path)
        os.remove(path)

    def create_csv(self, df):
        """Create a CSV file for the dataframe that can be loaded with COPY."""
        fd, path = tempfile.mkstemp(suffix='.csv', dir=g.TEMP)
        df.to_csv(path)
        os.chmod(path, FILE_MODE)
        return fd, path

    def copy_table(self, df, table, path):
        """Copy the table from a temp CSV file into the database."""
        columns = [f'"{c}"' for c in [df.index.name] + list(df.columns)]
        sql = f"""COPY {table} ({', '.join(columns)})
                  FROM '{path}' WITH (FORMAT csv, HEADER)"""
        self.execute(sql)

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
        super().bulk_add_setup()
        self.drop_constraints()

    def bulk_add_cleanup(self):
        """Prepare the database for use."""
        super().bulk_add_cleanup()
        self.add_constraints()

    def drop_constraints(self):
        """Drop constraints to speed up bulk data adds."""
        self.execute(
            f'ALTER TABLE counts DROP CONSTRAINT IF EXISTS counts_event_id')
        self.execute(
            f'ALTER TABLE counts DROP CONSTRAINT IF EXISTS counts_taxon_id')
        self.execute(
            f'ALTER TABLE events DROP CONSTRAINT IF EXISTS events_place_id')
        self.execute(
            f'ALTER TABLE places DROP CONSTRAINT IF EXISTS places_dataset_id')

    def add_constraints(self):
        """Add constraints in bulk."""
        sql = """
            ALTER TABLE places ADD CONSTRAINT places_dataset_id
            FOREIGN KEY (dataset_id) REFERENCES datasets (dataset_id)
        """
        self.execute(sql)

        sql = """
            ALTER TABLE events ADD CONSTRAINT events_place_id
            FOREIGN KEY (place_id) REFERENCES places (place_id)
        """
        self.execute(sql)

        sql = """
            ALTER TABLE counts ADD CONSTRAINT counts_event_id
            FOREIGN KEY (event_id) REFERENCES events (event_id)
        """
        self.execute(sql)

        sql = """
            ALTER TABLE counts ADD CONSTRAINT counts_taxon_id
            FOREIGN KEY (taxon_id) REFERENCES taxons (taxon_id)
        """
        self.execute(sql)
