#!/usr/bin/env python3

"""Ingest eBird data for a given bounding box."""

import argparse
import textwrap
from pathlib import Path

import pandas as pd

from pylib import util
from pylib.util import log

RAW_DIR = Path('data') / 'raw' / 'ebird'
RAW_CSV = 'ebd_relMay-2020.txt.gz'


def main(args):
    """Ingest eBird data."""
    chunk = 1_000_000
    reader = pd.read_csv(
        RAW_DIR / RAW_CSV,
        delimiter='\t',
        quoting=3,
        chunksize=chunk,
        dtype='unicode')

    first_chunk = True
    for i, raw_data in enumerate(reader, 1):
        log(f'Batch {i} x 1,000,000')
        raw_data = filter_data(args, raw_data)

        if raw_data.shape[0] == 0:
            continue

        if first_chunk:
            raw_data.to_csv(args.csv_file, index=False)
            first_chunk = False
        else:
            raw_data.to_csv(args.csv_file, index=False, mode='a', header=False)


def filter_data(args, raw_data):
    """Limit the size & scope of the data."""
    util.normalize_columns_names(raw_data)

    raw_data['OBSERVATION_DATE'] = pd.to_datetime(
        raw_data['OBSERVATION_DATE'], errors='coerce')

    has_date = raw_data['OBSERVATION_DATE'].notna()
    is_approved = raw_data['APPROVED'] == '1'
    is_complete = raw_data['ALL_SPECIES_REPORTED'] == '1'

    keep = raw_data['GLOBAL_UNIQUE_IDENTIFIER'].notna()
    if not args.no_observation_date:
        keep &= has_date
    if not args.unapproved:
        keep &= is_approved
    if not args.incomplete:
        keep &= is_complete

    raw_data = raw_data.loc[keep, :].copy()

    return util.filter_lng_lat(
        raw_data, 'LONGITUDE', 'LATITUDE', lng=args.longitude, lat=args.latitude)


def parse_args():
    """Process command-line arguments."""
    description = """Parse data from flora website."""
    arg_parser = argparse.ArgumentParser(
        description=textwrap.dedent(description),
        fromfile_prefix_chars='@')

    arg_parser.add_argument(
        '--csv-file', '-C', required=True,
        help="""Output the results to this CSV file.""")

    arg_parser.add_argument(
        '--longitude', '--lng', '--long', required=True, nargs=2, type=float,
        help="""Longitudes of the bounding box.""")

    arg_parser.add_argument(
        '--latitude', '--lat', required=True, nargs=2, type=float,
        help="""Latitudes of the bounding box.""")

    arg_parser.add_argument(
        '--no-observation-date', action='store_true',
        help="""Don't require records to have a valid date.""")

    arg_parser.add_argument(
        '--unapproved', action='store_true',
        help="""Don't require records to be approved""")

    arg_parser.add_argument(
        '--incomplete', action='store_true',
        help="""Don't require records to be complete""")

    args = arg_parser.parse_args()

    args.longitude = sorted(args.longitude)
    args.latitude = sorted(args.latitude)

    return args


if __name__ == '__main__':
    ARGS = parse_args()
    main(ARGS)
