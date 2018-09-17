"""Common dataframe ingest functions."""

import pandas as pd


def filter_lat_lng(df, lat=(-90.0, 90.0), lng=(-180.0, 180.0)):
    """Remove bad latitudes and longitudes."""
    df.lat = pd.to_numeric(
        df.lat, errors='coerce').fillna(9999.9).astype(float)
    df.lng = pd.to_numeric(
        df.lng, errors='coerce').fillna(9999.9).astype(float)
    good_lat = df.lat.between(lat[0], lat[1])
    good_lng = df.lng.between(lng[0], lng[1])

    return df.loc[good_lat & good_lng, :]


def add_taxon_genera_records(taxons):
    """Create genera records."""
    genera = taxons.groupby('genus').first().reset_index()
    genera.sci_name = genera.genus + ' sp.'
    genera.common_name = ''
    taxons = pd.concat([taxons, genera], sort=True)
    return taxons
