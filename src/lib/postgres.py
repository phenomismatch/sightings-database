"""Functions for dealing with the postgres database."""

import subprocess
from os import fspath
from pathlib import Path
import psycopg2
from sqlalchemy import create_engine


class Connection:
    """Postgresql functions."""

    ENGINE = 'postgresql://{}@localhost:5432/sightings'
    CONNECT = 'dbname=sightings user={}'
    CREATE_SCRIPT = fspath(Path('src') / 'sql' / 'postgres_01_create_db.sql')
    CREATE_CMD = f'psql -U postgres -d sightings -a -f {CREATE_SCRIPT}'

    @classmethod
    def create(cls):
        """Create the database."""
        print('Creating database')
        subprocess.check_call(cls.CREATE_CMD, shell=True)

    def __init__(self, user='postgres'):
        """Connect to the database and initialize parameters."""
        self.cxn = psycopg2.connect(self.CONNECT.format(user))
        self.engine = create_engine(self.ENGINE.format(user))

    def execute(self, sql, values=None):
        """Execute and commit the given query."""
        self.cxn.cursor().execute(sql, values)
        self.cxn.commit()
