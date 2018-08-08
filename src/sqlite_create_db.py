"""Create the SQLite3 version of the sightings database."""

from lib.base_create_db import BaseCreateDb
from lib.sqlite_db import SqliteDb


class SqliteCreateDb(BaseCreateDb):
    """Create a sightings database and input constant data."""


if __name__ == '__main__':
    SqliteCreateDb(SqliteDb).create_database()
