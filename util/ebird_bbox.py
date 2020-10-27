"""Ingest eBird data for Florida."""

import sys

sys.path.append('..')

from pathlib import Path

import pandas as pd

from pylib import util
from pylib.util import log

RAW_DIR = Path('..') / 'data' / 'raw' / 'ebird'
RAW_CSV = 'ebd_relMay-2020.txt.gz'

OUT_NAME = 'ebird_in_kenya_ebd_relMay-2020a.csv'
OUT_FILE = Path('..') / 'data' / 'interim' / OUT_NAME
LNG = (36.167185, 37.438853)
LAT = (-0.319235, 0.913940)


def ingest():
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
        raw_data = filter_data(raw_data)

        if raw_data.shape[0] == 0:
            continue

        if first_chunk:
            raw_data.to_csv(OUT_FILE, index=False)
        else:
            raw_data.to_csv(OUT_FILE, index=False, mode='a', header=False)

        first_chunk = False


def filter_data(raw_data):
    """Limit the size & scope of the data."""
    util.normalize_columns_names(raw_data)

    raw_data['OBSERVATION_DATE'] = pd.to_datetime(
        raw_data['OBSERVATION_DATE'], errors='coerce')

    has_date = raw_data['OBSERVATION_DATE'].notna()
    is_approved = raw_data['APPROVED'] == '1'
    is_complete = raw_data['ALL_SPECIES_REPORTED'] == '1'

    raw_data = raw_data.loc[has_date & is_approved & is_complete, :].copy()

    return util.filter_lng_lat(
        raw_data, 'LONGITUDE', 'LATITUDE', lng=LNG, lat=LAT)


if __name__ == '__main__':
    ingest()
