#!/usr/bin/env python3
"""Wrapper for sightings database extract, transform, & load functions."""

# pylint: disable=unused-argument

import argparse
import pylib.db as db
from pylib.util import log
import pylib.clements_ingest
import pylib.bbs_ingest
import pylib.maps_ingest
import pylib.ebird_ingest
import pylib.pollard_ingest
import pylib.naba_ingest
import pylib.nestwatch_ingest
import pylib.caterpillar_ingest


SEPARATOR = '*' * 80

DATASETS = [  # Order matters
    ('clements', lib.clements_ingest),
    ('bbs', lib.bbs_ingest),
    ('maps', lib.maps_ingest),
    ('nestwatch', lib.nestwatch_ingest),
    ('pollard', lib.pollard_ingest),
    ('naba', lib.naba_ingest),
    # ('caterpillar', lib.caterpillar_ingest),
    ('ebird', lib.ebird_ingest)]
DATASET_NAMES = [x[0] for x in DATASETS]
INGEST_OPTIONS = DATASET_NAMES + ['all']
EXPORTS = ['datasets', 'taxa'] + [x for x in DATASET_NAMES if x != 'clements']
EXPORT_OPTIONS = EXPORTS + ['all']


def parse_args():
    """Get user input."""
    parser = argparse.ArgumentParser(
        allow_abbrev=True,
        description='Extract, transform, & load data for the '
                    'sightings database.')
    subparsers = parser.add_subparsers()

    backup_parser = subparsers.add_parser(
        'backup', help="""Backup the SQLite3 database.""")
    backup_parser.set_defaults(func=backup)

    create_parser = subparsers.add_parser(
        'create', help="""Create the SQLite3 database tables & indices.""")
    create_parser.set_defaults(func=create)

    ingest_parser = subparsers.add_parser(
        'ingest', aliases=['i', 'in'],
        help="""Ingest a dataset into the SQLite3 database.""")
    ingest_parser.add_argument(
        'datasets', nargs='+', choices=INGEST_OPTIONS,
        help=f"""Ingest a dataset into the SQLite3 database.
            Note: 'all' will ingest all datasets.""")
    ingest_parser.set_defaults(func=ingest)

    csv_parser = subparsers.add_parser(
        'export',
        help="""Export data from an SQLite3 database to CSV files.""")
    csv_parser.add_argument(
        'datasets', nargs='+', choices=EXPORT_OPTIONS,
        help="""Export a dataset or table to CSV file(s).
            Note: 'all' will export everything.""")
    csv_parser.add_argument(
        'path', help="""Export the CSV files to this directory.""")
    csv_parser.set_defaults(func=export)

    postgres_parser = subparsers.add_parser(
        'postgres', help="""Create the PostgreSQL database.""")
    postgres_parser.set_defaults(func=postgres)

    load_parser = subparsers.add_parser(
        'import', help="""Import CSV files into the PostgreSQL database.""")
    load_parser.set_defaults(func=import_)

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
        args.datasets = INGEST_OPTIONS

    # Order matters
    for _ingest, module in DATASETS:
        if _ingest in args.datasets:
            log(SEPARATOR)
            module.ingest()
    log(SEPARATOR)


def export(args):
    """Export the SQLite3 database to CSV files."""
    if 'all' in args.datasets:
        args.datasets = EXPORTS
    for item in args.datasets:
        log(SEPARATOR)
        db.export_to_csv_files(item, args.path)
    log(SEPARATOR)


def postgres(args):
    """Create the PostgreSQL database."""
    db.create_postgres()


def import_(args):
    """Import the CSV files into the PostgreSQL database."""
    db.import_postgres()


def etl():
    """Do the selected actions."""
    args = parse_args()
    args.func(args)
    log('Done')


if __name__ == '__main__':
    etl()
