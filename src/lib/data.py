"""Common dataframe ingest functions."""

import pandas as pd
import lib.sqlite as db


def add_event_id(events, cxn):
    """Add event IDs to the dataframe."""
    event_id = db.next_id(cxn, 'events')
    events['event_id'] = range(event_id, event_id + events.shape[0])
    return events.set_index('event_id')


def add_count_id(counts, cxn):
    """Add count IDs to the dataframe."""
    count_id = db.next_id(cxn, 'counts')
    counts['count_id'] = range(count_id, count_id + counts.shape[0])
    return counts.set_index('count_id')


def insert_events(events, cxn, sidecar):
    """Insert the events into the database."""
    events.loc[:, db.EVENT_COLUMNS].to_sql('events', cxn, if_exists='append')
    insert_sidecar(events, sidecar, cxn, db.EVENT_COLUMNS)


def insert_counts(counts, cxn, sidecar):
    """Insert the counts into the database."""
    counts.loc[:, db.COUNT_COLUMNS].to_sql('counts', cxn, if_exists='append')
    insert_sidecar(counts, sidecar, cxn, db.COUNT_COLUMNS)


def insert_sidecar(df, sidecar, cxn, exclude):
    """Insert the events sidecar table into the database."""
    columns = [c for c in df.columns if c not in exclude + ['key']]
    df.loc[:, columns].to_sql(sidecar, cxn, if_exists='append')


def make_key_event_id_dict(events, *columns):
    """Create a key to event ID map so that we can link counts to events."""
    events['key'] = tuple(zip(*columns))
    return events.reset_index().set_index('key').event_id.to_dict()


def map_keys_to_event_ids(counts, keys, *columns):
    """Map the key to the event ID."""
    counts['key'] = tuple(zip(*columns))
    counts['event_id'] = counts.key.map(keys)
    has_event = counts.event_id.notna()
    return counts[has_event].copy()


def map_to_taxon_ids(df, column, taxons):
    """Map the given column to taxon IDs."""
    df['taxon_id'] = df[column].map(taxons)
    return df.loc[df.taxon_id.notna(), :]


def filter_lat_lng(df, lat=(-90.0, 90.0), lng=(-180.0, 180.0)):
    """Remove bad latitudes and longitudes."""
    df.latitude = pd.to_numeric(
        df.latitude, errors='coerce').fillna(9999.9).astype(float)
    df.longitude = pd.to_numeric(
        df.longitude, errors='coerce').fillna(9999.9).astype(float)
    good_lat = df.latitude.between(lat[0], lat[1])
    good_lng = df.longitude.between(lng[0], lng[1])

    return df.loc[good_lat & good_lng, :]
