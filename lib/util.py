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


def normalize_columns_names(dfn):
    """Remove problem characters from dataframe columns."""
    dfn.rename(columns=lambda x: re.sub(r'\W', '_', x), inplace=True)
    dfn.rename(columns=lambda x: re.sub(r'__', '_', x), inplace=True)
    dfn.rename(columns=lambda x: re.sub(r'^_|_$', '', x), inplace=True)


def json_object(dfn, fields):
    """Build an array of json objects from the dataframe fields."""
    dfn = dfn.fillna('')
    json_array = []
    for row in dfn.itertuples():
        obj = {}
        for field in fields:
            value = getattr(row, field)
            if value != '':
                obj[field] = value
        json_array.append(json.dumps(obj))
    return json_array


def update_json(row, fields):
    """Add data to the taxon_json field."""
    fields = 'species_id aou french_common_name spanish_common_name'.split()
    taxon_json = json.loads(row.taxon_json)
    for field in fields:
        if row[field]:
            taxon_json[field] = row[field]
    return json.dumps(taxon_json, ensure_ascii=False)


def filter_lng_lat(
        dfn, lng_col, lat_col, lng=(-180.0, 180.0), lat=(-90.0, 90.0)):
    """Remove bad latitudes and longitudes."""
    dfn[lng_col] = pd.to_numeric(
        dfn[lng_col], errors='coerce').fillna(9999.9).astype(float)
    dfn[lat_col] = pd.to_numeric(
        dfn[lat_col], errors='coerce').fillna(9999.9).astype(float)
    good_lng = dfn[lng_col].between(lng[0], lng[1])
    good_lat = dfn[lat_col].between(lat[0], lat[1])

    return dfn[good_lng & good_lat]
