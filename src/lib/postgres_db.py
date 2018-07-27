"""Functions for dealing with the postgres database connections."""

import re
import subprocess
from os import fspath
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine
from lib.base_db import BaseDb


class PostgresDb(BaseDb):
    """Postgresql connection."""

    ENGINE = 'postgresql://{}@localhost:5432/sightings'
    CONNECT = 'dbname=sightings user={}'
    CREATE_SCRIPT = fspath(Path('src') / 'sql' / 'postgres_01_create_db.sql')
    CREATE_CMD = f'psql -U postgres -d sightings -a -f {CREATE_SCRIPT}'

    @classmethod
    def create(cls):
        """Create the database."""
        print('Creating database')
        subprocess.check_call(cls.CREATE_CMD, shell=True)

    def __init__(self, user='postgres', dataset_id=None):
        """Connect to the database and initialize parameters."""
        self.cxn = psycopg2.connect(self.CONNECT.format(user))
        self.engine = create_engine(self.ENGINE.format(user))
        self.dataset_id = dataset_id

    def execute(self, sql, values=None):
        """Execute and commit the given query."""
        sql = re.sub(r'\?', '%s', sql)
        self.cxn.cursor().execute(sql, values)
        self.cxn.commit()

    def insert_sidecar(self, df, sidecar, exclude, index):
        """Insert the sidecar table into the database."""
        super().insert_sidecar(df, sidecar, exclude, index)
        self.execute(f'ALTER TABLE {sidecar} ADD PRIMARY KEY ({index})')

    def next_id(self, table):
        """Get the max value from the table's field."""
        field = table[:-1] + '_id'
        sql = 'SELECT COALESCE(MAX({}), 0) AS id FROM {}'.format(field, table)
        with self.cxn.cursor() as cur:
            cur.execute(sql)
            max_id = cur.fetchone()[0]
        return max_id + 1

    def bulk_add_setup(self):
        """Prepare the database for bulk adds."""
        super().bulk_add_setup()
        self.drop_constraints()

    def bulk_add_teardown(self):
        """Prepare the database for use."""
        super().bulk_add_teardown()
        self.add_constraints()

    def drop_constraints(self):
        """Drop constraints to speed up bulk data adds."""
        self.execute(f'ALTER TABLE counts DROP CONSTRAINT counts_event_id')
        self.execute(f'ALTER TABLE counts DROP CONSTRAINT counts_taxon_id')
        self.execute(f'ALTER TABLE events DROP CONSTRAINT events_place_id')
        self.execute(f'ALTER TABLE places DROP CONSTRAINT places_dataset_id')

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
