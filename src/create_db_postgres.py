"""Create the Postgres version of the sightings database."""

from lib.create_db import CreateDb
from lib.db_postgres import DbPostgres


class CreateDbPostgres(CreateDb):
    """Create a sightings database and input constant data."""

    def _insert_countries(self):
        super()._insert_countries()
        self.cxn.execute('ALTER TABLE countries ADD PRIMARY KEY (country_id)')


if __name__ == '__main__':
    CreateDbPostgres(DbPostgres).create_database()
