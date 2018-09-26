"""Wrapper for sightings database extract, tranform, & load functions."""

import argparse
import lib.db as db
from lib.log import log
import lib.countries_ingest
import lib.clements_ingest
import lib.bbs_ingest
import lib.maps_ingest
import lib.ebird_ingest
import lib.pollard_ingest
import lib.naba_ingest
import lib.caterpillar_ingest


INGESTS = [
    ('countries', lib.countries_ingest),
    ('clements', lib.clements_ingest),
    ('bbs', lib.bbs_ingest),
    ('maps', lib.maps_ingest),
    ('pollard', lib.pollard_ingest),
    ('naba', lib.naba_ingest),
    ('caterpillar', lib.caterpillar_ingest),
    ('ebird', lib.ebird_ingest)]
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
    parser.add_argument('-a', '--ingest-all', action='store_true',
                        help="""Ingest all datasets.""")
    parser.add_argument('--csv', action='store_true',
                        help="""Export the SQLite3 database to CSV files.""")
    parser.add_argument('--postgres', action='store_true',
                        help="""Import the CSV files into the PostgreSQL
                            database.""")
    return parser.parse_args()


def etl():
    """Do the selected actions."""
    separator = '*****************************************'

    args = parse_args()

    if args.backup:
        db.backup_database()

    if args.create_db:
        db.create()
        db.insert_version()

    if args.ingest_all:
        args.ingest = OPTIONS

    if args.ingest:
        for ingest, module in INGESTS:
            if ingest in args.ingest:
                log(separator)
                module.ingest()
        log(separator)

    if args.csv:
        db.export_to_csv_files()

    if args.postgres:
        db.create_postgres()

    log('Done')


if __name__ == '__main__':
    etl()
