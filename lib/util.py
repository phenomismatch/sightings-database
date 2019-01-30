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


def normalize_columns_names(dfm):
    """Remove problem characters from dataframe columns."""
    dfm.rename(columns=lambda x: re.sub(r'\W', '_', x), inplace=True)
    dfm.rename(columns=lambda x: re.sub(r'__', '_', x), inplace=True)
    dfm.rename(columns=lambda x: re.sub(r'^_|_$', '', x), inplace=True)


def json_object(dfm, fields):
    """Build an array of json objects from the dataframe fields."""
    dfm = dfm.fillna('')
    json_array = []
    for row in dfm.itertuples():
        obj = {}
        for field in fields:
            value = getattr(row, field)
            if value != '':
                obj[field] = value
        json_array.append(json.dumps(obj))
    return json_array


def update_json(row, fields):
    """Add data to the taxon_json field."""
    taxon_json = json.loads(row.taxon_json)
    for field in fields:
        if row[field]:
            taxon_json[field] = row[field]
    return json.dumps(taxon_json, ensure_ascii=False)


def filter_lng_lat(
        dfm, lng_col, lat_col, lng=(-180.0, 180.0), lat=(-90.0, 90.0)):
    """Remove bad latitudes and longitudes."""
    dfm[lng_col] = pd.to_numeric(
        dfm[lng_col], errors='coerce').fillna(9999.9).astype(float)
    dfm[lat_col] = pd.to_numeric(
        dfm[lat_col], errors='coerce').fillna(9999.9).astype(float)
    good_lng = dfm[lng_col].between(lng[0], lng[1])
    good_lat = dfm[lat_col].between(lat[0], lat[1])

    return dfm[good_lng & good_lat]
