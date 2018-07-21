"""Create the Postgres version of the sightings database."""

from lib.base_01_create_db import BaseCreateDb
from lib.postgres import Connection


class PostgresCreateDb(BaseCreateDb):
    """Create a sightings database and input constant data."""

    def _insert_countries(self):
        super()._insert_countries()
        self.cxn.execute('ALTER TABLE countries ADD PRIMARY KEY (country_id)')


if __name__ == '__main__':
    PostgresCreateDb(Connection).create_database()
