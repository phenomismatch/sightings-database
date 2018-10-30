#!/usr/bin/env python

"""Wrapper for sightings database extract, transform, & load functions."""

import argparse
import lib.db as db
from lib.util import log
import lib.clements_ingest
import lib.bbs_ingest
import lib.maps_ingest
import lib.ebird_ingest
import lib.pollard_ingest
import lib.naba_ingest
import lib.caterpillar_ingest


SEPARATOR = '*****************************************'

DATASETS = [  # Order matters
    ('clements', lib.clements_ingest),
    ('bbs', lib.bbs_ingest),
    ('maps', lib.maps_ingest),
    ('pollard', lib.pollard_ingest),
    ('naba', lib.naba_ingest),
    ('caterpillar', lib.caterpillar_ingest),
    ('ebird', lib.ebird_ingest)]
OPTIONS = [i[0] for i in DATASETS] + ['all']


def parse_args():
    """Get user input."""
    parser = argparse.ArgumentParser(
        allow_abbrev=True,
        description='Extract, transform, & load data for the '
                    'sightings database.')
    subparsers = parser.add_subparsers()

    backup_parser = subparsers.add_parser(
        'backup',
        help="""Backup the SQLite3 database.""")
    backup_parser.set_defaults(func=backup)

    create_parser = subparsers.add_parser(
        'create',
        help="""Create the SQLite3 database tables & indices.""")
    create_parser.set_defaults(func=create)

    ingest_parser = subparsers.add_parser(
        'ingest',
        help=f"""Ingest a dataset into the SQLite3 database.""")
    ingest_parser.add_argument(
        'datasets',
        nargs='+',
        metavar='DATASET',
        choices=OPTIONS,
        help=f"""Ingest a dataset into the SQLite3 database.
            Options: { ", ".join(OPTIONS) }. Note: 'all' will ingest all of the
            other datasets.""")
    ingest_parser.set_defaults(func=ingest)

    create_parser = subparsers.add_parser(
        'csv',
        help="""Export the SQLite3 database to CSV files.""")
    create_parser.set_defaults(func=csv)

    postgres_parser = subparsers.add_parser(
        'postgres',
        help="""Create the PostgreSQL database.""")
    postgres_parser.set_defaults(func=postgres)

    load_parser = subparsers.add_parser(
        'load',
        help="""Load CSV files into the PostgreSQL database.""")
    load_parser.set_defaults(func=load)

    return parser.parse_args()


def backup(args):
    """Backup the SQLite3 database."""
    db.backup_database()


def create(args):
    """Create the SQLite3 database."""
    db.create()


def ingest(args):
    """Ingest datasets into the SQLite3 database."""
    if 'all' in args.datasets:
        args.datasets = OPTIONS

    # Order matters
    for ingest, module in DATASETS:
        if ingest in args.ingest:
            log(SEPARATOR)
            module.ingest()
    log(SEPARATOR)


def csv(args):
    """Export the SQLite3 database to CSV files."""
    db.export_to_csv_files()


def postgres(args):
    """Create the PostgreSQL database."""
    db.create_postgres()


def load(args):
    """Load the CSV files into the PostgreSQL database."""
    db.load_postgres()


def etl():
    """Do the selected actions."""
    args = parse_args()
    args.func(args)
    log('Done')


if __name__ == '__main__':
    etl()
