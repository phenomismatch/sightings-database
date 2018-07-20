"""Common dataframe ingest functions."""

import pandas as pd
import lib.sqlite as db


def add_event_id(dates, cxn):
    """Add event IDs to the dataframe."""
    date_id = db.next_id(cxn, 'dates')
    dates['date_id'] = range(date_id, date_id + dates.shape[0])
    return dates.set_index('date_id')


def add_count_id(counts, cxn):
    """Add count IDs to the dataframe."""
    count_id = db.next_id(cxn, 'counts')
    counts['count_id'] = range(count_id, count_id + counts.shape[0])
    return counts.set_index('count_id')


def insert_events(dates, cxn, sidecar):
    """Insert the dates into the database."""
    dates.loc[:, db.EVENT_COLUMNS].to_sql('dates', cxn, if_exists='append')
    insert_sidecar(dates, sidecar, cxn, db.EVENT_COLUMNS)


def insert_counts(counts, cxn, sidecar):
    """Insert the counts into the database."""
    counts.loc[:, db.COUNT_COLUMNS].to_sql('counts', cxn, if_exists='append')
    insert_sidecar(counts, sidecar, cxn, db.COUNT_COLUMNS)


def insert_sidecar(df, sidecar, cxn, exclude):
    """Insert the dates sidecar table into the database."""
    columns = [c for c in df.columns if c not in exclude + ['key']]
    df.loc[:, columns].to_sql(sidecar, cxn, if_exists='append')


def make_key_event_id_dict(dates, *columns):
    """Create a key to event ID map so that we can link counts to dates."""
    dates['key'] = tuple(zip(*columns))
    return dates.reset_index().set_index('key').date_id.to_dict()


def map_keys_to_event_ids(counts, keys, *columns):
    """Map the key to the event ID."""
    counts['key'] = tuple(zip(*columns))
    counts['date_id'] = counts.key.map(keys)
    has_event = counts.date_id.notna()
    return counts[has_event].copy()


def map_to_taxon_ids(df, column, taxons):
    """Map the given column to taxon IDs."""
    df['taxon_id'] = df[column].map(taxons)
    return df.loc[df.taxon_id.notna(), :]


def filter_lat_lng(df, lat=(-90.0, 90.0), lng=(-180.0, 180.0)):
    """Remove bad latitudes and longitudes."""
    df.lat = pd.to_numeric(
        df.lat, errors='coerce').fillna(9999.9).astype(float)
    df.lng = pd.to_numeric(
        df.lng, errors='coerce').fillna(9999.9).astype(float)
    good_lat = df.lat.between(lat[0], lat[1])
    good_lng = df.lng.between(lng[0], lng[1])

    return df.loc[good_lat & good_lng, :]
