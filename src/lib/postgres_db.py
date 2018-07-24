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

    def __init__(self, user='postgres'):
        """Connect to the database and initialize parameters."""
        self.cxn = psycopg2.connect(self.CONNECT.format(user))
        self.engine = create_engine(self.ENGINE.format(user))

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
