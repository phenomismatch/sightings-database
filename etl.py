"""Wrapper for sightings database extract, tranform, & load functions."""

import argparse
import subprocess
from datetime import datetime
import lib.db as db
import lib.util as util
import lib.countries_ingest
import lib.clements_ingest
import lib.bbs_ingest


INGESTS = [
    ('countries', lib.countries_ingest),
    ('clements', lib.clements_ingest),
    ('bbs', lib.bbs_ingest),
    ('maps', None),
    ('naba', None),
    ('ebird', None),
    ('pollard', None),
    ('caterpillar', None)]
OPTIONS = [i[0] for i in INGESTS]


def parse_args():
    """Get user input."""
    parser = argparse.ArgumentParser(
        description='Extract, transform, & load data for the '
                    'sightings database.')
    parser.add_argument('-b', '--backup', action='store_true',
                        help="""Backup the SQLite3 database?""")
    parser.add_argument('-c', '--create-db', action='store_true',
                        help="""Create the SQLite3 database tables &
                            indices?""")
    parser.add_argument('-i', '--ingest', nargs='+',
                        metavar='DATASET', choices=OPTIONS,
                        help=f"""Ingest a dataset into the SQLite3 database.
                            Options: { ", ".join(OPTIONS) }""")
    return parser.parse_args()


def etl():
    """Do the selected actions."""
    args = parse_args()

    if args.backup:
        now = datetime.now()
        backup = f'{db.DB_PATH}_{now.strftime("%Y-%m-%d")}.sql'
        cmd = f'cp {db.DB_PATH} {backup}'
        subprocess.check_call(cmd, shell=True)

    if args.create_db:
        db.create()
        db.insert_version()

    if args.ingest:
        for ingest, module in INGESTS:
            if ingest in args.ingest:
                util.log('*********************************')
                module.ingest()

    util.log('Done')


if __name__ == "__main__":
    etl()
