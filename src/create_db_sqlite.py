"""Create the SQLite3 version of the sightings database."""

from lib.create_db import CreateDb
from lib.db_sqlite import DbSqlite


class CreateDbSqlite(CreateDb):
    """Create a sightings database and input constant data."""


if __name__ == '__main__':
    CreateDbSqlite(DbSqlite).create_database()
