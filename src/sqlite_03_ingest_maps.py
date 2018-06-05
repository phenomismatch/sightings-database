"""Ingest Monitoring Avian Productivity and Survivorship data."""

import os
from datetime import date
import pandas as pd
from simpledbf import Dbf5
import geohash2
import lib.sqlite as db
import lib.data as data


DATASET_ID = 'maps'
MAPS_PATH = db.DATA_DIR / 'raw' / DATASET_ID


def ingest_maps():
    """Ingest MAPS data."""
    cxn = db.connect()

    convert_dbf_to_csv('LIST17.DBF', 'LIST17.csv')
    convert_dbf_to_csv('1117BAND.DBF', '1117band.csv')
    convert_dbf_to_csv('1117EF.DBF', '1117ef.csv')

    db.delete_dataset(cxn, DATASET_ID)
    insert_dataset(cxn)
    insert_codes(cxn)

    taxons = select_taxons(cxn)
    stations = read_stations()
    effort = read_effort()
    bands = read_bands(taxons, stations)

    keys = insert_events(cxn, effort, stations)
    insert_counts(cxn, bands, keys, taxons)
    db.update_points(cxn, DATASET_ID)


def convert_dbf_to_csv(dbf_file, csv_file):
    """Convert the big DBF files to CSV format."""
    if not os.path.exists(MAPS_PATH / csv_file):
        print(f'Converting {dbf_file} files to {csv_file}')
        dbf = Dbf5(MAPS_PATH / dbf_file)
        df = dbf.to_dataframe()
        df.to_csv(MAPS_PATH / csv_file, index=False)


def select_taxons(cxn):
    """Select taxons and return a dictionary of aou and taxon_ids."""
    print('Reading taxons')

    sql = """SELECT taxon_id, sci_name FROM taxons"""
    ids = pd.read_sql(sql, cxn).set_index('sci_name').taxon_id.to_dict()

    dbf = Dbf5(MAPS_PATH / 'LIST17.DBF')
    specs = dbf.to_dataframe().set_index('SCINAME').SPEC.to_dict()

    return {sid: ids[nm] for nm, sid in specs.items() if nm in ids}


def insert_dataset(cxn):
    """Insert a dataset record."""
    print('Inserting dataset')

    dataset = dict(
        dataset_id=DATASET_ID,
        title='MAPS: Monitoring Avian Productivity and Survivorship',
        extracted=str(date.today()),
        version='2017.0',
        url='https://www.birdpop.org/pages/maps.php')
    return db.insert_dataset(cxn, dataset)


def insert_codes(cxn):
    """Insert MAPS code values into the database."""
    print('Inserting codes')

    codes = pd.read_csv(MAPS_PATH / 'maps_codes.csv')
    codes.to_sql('maps_codes', cxn, if_exists='replace', index=False)


def read_effort():
    """ETL the effort records."""
    print('Reading effort')

    effort = pd.read_csv(MAPS_PATH / '1117ef.csv', dtype='unicode')
    effort = effort.groupby(['STA', 'DATE']).agg({'START': min, 'END': max})
    effort = effort.rename(columns={'START': 'start_time', 'END': 'end_time'})

    convert_to_time(effort, 'start_time')
    convert_to_time(effort, 'end_time')

    return effort


def convert_to_time(df, column):
    """Convert the time field from string HHm format to HH:MM format."""
    is_na = df[column].isna()
    df[column] = df[column].fillna('0').astype(str)
    df[column] = df[column].str.pad(4, fillchar='0', side='right')
    df[column] = pd.to_datetime(
        df[column], format='%H%M', errors='coerce').dt.strftime('%H:%M')
    df.loc[is_na, column] = None


def read_stations():
    """ETL the stations dataframe."""
    print('Reading stations')

    keep = """STATION LOC STA STA2 NAME LHOLD HOLDCERT O NEARTOWN COUNTY STATE
        US REGION BLOCK LATITUDE LONGITUDE PRECISION SOURCE DATUM DECLAT DECLNG
        NAD83 ELEV STRATUM BCR HABITAT REG PASSED""".split()

    stations = pd.read_excel(MAPS_PATH / 'STATIONS.xlsx', dtype='unicode')
    stations = stations.drop(columns=[
        c for c in stations.columns if c not in keep], axis=1)

    rename = {'DECLAT': 'latitude', 'DECLNG': 'longitude'}
    stations = stations.rename(columns=rename)

    radii = {
        '01S': 30.92,
        '10S': 30.92 * 10,
        'BLK': 111.32 * 1000 * 10,
        '01M': 111.32 * 1000,
        '05S': 30.92 * 5,
        '10M': 111.32 * 1000 * 10}
    stations['radius'] = stations.PRECISION.map(radii)

    stations.STA = stations.STA.fillna('').astype(str)
    good_sta = stations.STA.str.len() > 3
    stations = stations.loc[good_sta, :]

    stations = data.filter_lat_lng(stations)

    return stations


def read_bands(taxons, stations):
    """ETL the bands dataset."""
    print('Reading bands')

    bands = pd.read_csv(MAPS_PATH / '1117band.csv', dtype='unicode')
    bands = data.map_to_taxon_ids(bands, 'SPEC', taxons)

    return bands


def insert_events(cxn, effort, stations):
    """Insert event records."""
    print('Inserting events')

    events = effort.reset_index(level=['STA', 'DATE'])
    events = events.merge(right=stations, how='inner', on='STA')

    events = data.add_event_id(events, cxn)
    events['dataset_id'] = DATASET_ID
    events.DATE = pd.to_datetime(events.DATE)
    events['year'] = events.DATE.dt.strftime('%Y')
    events['day'] = events.DATE.dt.strftime('%j')
    events['geohash'] = events.apply(lambda x: geohash2.encode(
        x.latitude, x.longitude, precision=7), axis=1)
    keys = data.make_key_event_id_dict(events, events.STA, events.DATE)
    events.DATE = events.DATE.dt.date

    data.insert_events(events, cxn, 'maps_events')

    return keys


def insert_counts(cxn, counts, keys, taxons):
    """Insert count and count detail records."""
    print('Inserting counts')

    counts = data.map_keys_to_event_ids(
        counts, keys, counts.STA, pd.to_datetime(counts.DATE))

    counts = data.add_count_id(counts, cxn)
    counts['count'] = 1

    data.insert_counts(counts, cxn, 'maps_counts')


if __name__ == '__main__':
    ingest_maps()
