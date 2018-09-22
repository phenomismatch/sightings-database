"""Utiliites & constants."""

import json
import pandas as pd
import lib.db as db


def json_object(df, fields, dataset_id=None):
    """Build an array of json objects from the dataframe fields."""
    df = df.fillna('')
    json_array = []
    for row in df.itertuples():
        obj = {}
        if dataset_id:
            obj['dataset_id'] = dataset_id
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

    return df.loc[good_lng & good_lat, :]


def drop_duplicate_taxons(taxons):
    """Update the taxons dataframe and add them to the taxons CSV file."""
    cxn = db.connect()
    existing = pd.read_sql('SELECT sci_name, taxon_id FROM taxons', cxn)
    existing = existing.set_index('sci_name').taxon_id.to_dict()
    in_existing = taxons.sci_name.isin(existing)
    return taxons.loc[~in_existing, :].drop_duplicates('sci_name').copy()


def add_taxon_genera_records(taxons):
    """Create genera records."""
    genera = taxons.groupby('genus').first().reset_index()
    genera.sci_name = genera.genus + ' sp.'
    genera.common_name = ''
    taxons = pd.concat([taxons, genera], sort=True)
    return taxons
