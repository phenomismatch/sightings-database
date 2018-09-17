"""Wrapper to invoke sightings database build functions."""

import argparse
import subprocess
from datetime import datetime
from lib.db_sqlite import DbSqlite
from lib.db_postgres import DbPostgres
from lib.create_db import CreateDbPostgres, CreateDbSqlite
from lib.bbs_ingest import BbsIngestPostgres, BbsIngestSqlite
from lib.maps_ingest import MapsIngestPostgres, MapsIngestSqlite
from lib.naba_ingest import NabaIngestPostgres, NabaIngestSqlite
from lib.ebird_ingest import EbirdIngestPostgres, EbirdIngestSqlite
from lib.pollard_ingest import PollardIngestPostgres, PollardIngestSqlite


MODULE = {
    'create': (CreateDbPostgres, CreateDbSqlite),
    'bbs': (BbsIngestPostgres, BbsIngestSqlite),
    'maps': (MapsIngestPostgres, MapsIngestSqlite),
    'naba': (NabaIngestPostgres, NabaIngestSqlite),
    'ebird': (EbirdIngestPostgres, EbirdIngestSqlite),
    'pollard': (PollardIngestPostgres, PollardIngestSqlite)}

INGESTS = ['bbs', 'maps', 'ebird', 'pollard', 'naba', 'catcounts']


def parse_args():
    """Get user input."""
    parser = argparse.ArgumentParser(
        description='Build the sightings database.')

    parser.add_argument('-d', '--db', default="sqlite",
                        choices=['sqlite', 'postgres'],
                        help='''Which database?''')
    parser.add_argument('-c', '--clean-db', action='store_true',
                        help='''Clean the database?''')
    parser.add_argument('-B', '--backup-db', action='store_true',
                        help='''Backup the database?''')
    parser.add_argument('-b', '--build-db', action='store_true',
                        help='''Build the database?''')
    parser.add_argument('-i', '--ingest', nargs='*',
                        choices=INGESTS,
                        help='''Ingest datasets? Options="{}"'''.format(
                            ', '.join(INGESTS)))

    return parser.parse_args()


def main():
    """Do the selected actions."""
    args = parse_args()

    which_db = int(args.db == 'sqlite')  # 0=postgres, 1=sqlite
    db = DbSqlite if which_db else DbPostgres

    if args.backup_db and which_db:
        now = datetime.now()
        backup = f'{DbSqlite.DB_PATH}_{now.strftime("%Y-%m-%d")}.sql'
        cmd = f'cp {DbSqlite.DB_PATH} {backup}'
        subprocess.check_call(cmd, shell=True)

    if args.clean_db and which_db:
        cmd = f'rm -f {DbSqlite.DB_PATH}'
        subprocess.check_call(cmd, shell=True)

    if args.build_db:
        module = MODULE['create'][which_db]
        module(db).create_database()

    if args.ingest:
        for ingest in args.ingest:
            module = MODULE[ingest][which_db]
            module(db).ingest()


if __name__ == "__main__":
    main()
