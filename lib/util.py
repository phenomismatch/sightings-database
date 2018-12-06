"""Utilities & constants."""

import re
import json
from datetime import datetime
import pandas as pd


def log(msg):
    """Log a status message."""
    now = datetime.now().strftime('%Y-%M-%d %H:%M:%S')
    msg = f'{now} {msg}'
    print(msg)


def normalize_columns_names(df):
    """Remove problem characters from dataframe columns."""
    df.rename(columns=lambda x: re.sub(r'\W', '_', x), inplace=True)
    df.rename(columns=lambda x: re.sub(r'__', '_', x), inplace=True)
    df.rename(columns=lambda x: re.sub(r'^_|_$', '', x), inplace=True)


def json_object(df, fields):
    """Build an array of json objects from the dataframe fields."""
    df = df.fillna('')
    json_array = []
    for row in df.itertuples():
        obj = {}
        for field in fields:
            value = getattr(row, field)
            if value != '':
                obj[field] = value
        json_array.append(json.dumps(obj))
    return json_array


def filter_lng_lat(
        df, lng_col, lat_col, lng=(-180.0, 180.0), lat=(-90.0, 90.0)):
    """Remove bad latitudes and longitudes."""
    df[lng_col] = pd.to_numeric(
        df[lng_col], errors='coerce').fillna(9999.9).astype(float)
    df[lat_col] = pd.to_numeric(
        df[lat_col], errors='coerce').fillna(9999.9).astype(float)
    good_lng = df[lng_col].between(lng[0], lng[1])
    good_lat = df[lat_col].between(lat[0], lat[1])

    return df[good_lng & good_lat]
