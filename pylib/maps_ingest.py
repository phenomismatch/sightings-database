"""
Ingest Monitoring Avian Productivity and Survivorship (MAPS) data.

This is a bird banding survey, so there is going to be a count of one for each
observation. The data is broken into 5 files:
1) A list of species.
2) A list of banding stations, which correspond to the places table.
3) An effort table containing information about the banding conditions. This
    corresponds with the events table.
4) The band table which contains information about the bird's condition. This
    corresponds to the counts table. A count of one is assumed.
5) The breeding status for a species of birds at a station during a year.
"""

import os
from pathlib import Path
import pandas as pd
from simpledbf import Dbf5
from . import db
from .util import log, json_object


DATASET_ID = 'maps'
RAW_DIR = Path('data') / 'raw' / DATASET_ID
LIST = 'LIST18'
BAND = '1016B19'
EFFORT = '1016E19'
STATUS = '1016M19'
STATIONS = 'STATIONS'


def ingest():
    """Ingest the data."""
    convert_dbf_to_csv(LIST)
    convert_dbf_to_csv(BAND)
    convert_dbf_to_csv(STATUS)
    convert_dbf_to_csv(EFFORT)

    db.delete_dataset_records(DATASET_ID)

    db.insert_dataset({
        'dataset_id': DATASET_ID,
        'title': 'MAPS: Monitoring Avian Productivity and Survivorship',
        'version': '2019.0',
        'url': 'https://www.birdpop.org/pages/maps.php'})

    insert_stations()
    insert_status()
    insert_effort()
    insert_bands()

    insert_taxa()
    insert_places()
    insert_events()
    insert_counts()


def convert_dbf_to_csv(file_name):
    """Convert DBF files to CSV files."""
    csv_file = RAW_DIR / f'{file_name}.csv'
    dbf_file = RAW_DIR / f'{file_name}.DBF'
    if not os.path.exists(csv_file):
        log(f'Converting {dbf_file} files to {csv_file}')
        dbf = Dbf5(dbf_file)
        df = dbf.to_dataframe()
        df.to_csv(csv_file, index=False)


def insert_taxa():
    """Insert MAPS taxon data."""
    log(f'Inserting {DATASET_ID} taxa')

    df = pd.read_csv(RAW_DIR / f'{LIST}.csv')
    df['SCINAME'] = df['SCINAME'].str.split().str.join(' ')
    df['GENUS'] = df['SCINAME'].str.split().str[0]
    df.to_sql('maps_list', db.connect(), if_exists='replace', index=False)

    # Look for taxa that are not already in the database. We are looking for
    # SPEC codes in the maps taxa table (maps_list) not in the database. Then
    # we insert the missing ones.
    sql = """
        WITH new_taxa AS (SELECT * FROM maps_list
                           WHERE spec NOT IN (SELECT spec FROM taxa))
        INSERT INTO taxa
            (class, genus, sci_name, common_name, spec)
        SELECT 'aves'     AS class,
               genus,
               sciname    AS sci_name,
               commonname AS common_name,
               spec
          FROM new_taxa;
        """
    with db.connect() as cxn:
        cxn.execute(sql)
        cxn.commit()


def insert_stations():
    """Build a table to hold the raw MAPS stations data."""
    log(f'Inserting {DATASET_ID} stations')

    df = pd.read_csv(RAW_DIR / f'{STATIONS}.csv', dtype='unicode')
    df['place_id'] = db.create_ids(df, 'places')
    df['place_json'] = json_object(df, """STATION LOC STA STA2 NAME LHOLD
        HOLDCERT O NEARTOWN COUNTY STATE US REGION BLOCK LATITUDE LONGITUDE
        PRECISION SOURCE DATUM DECLAT DECLNG NAD83 ELEV STRATUM BCR HABITAT
        REG PASSED""".split())
    df.to_sql('maps_stations', db.connect(), if_exists='replace', index=False)


def insert_effort():
    """Build a table to hold the raw MAPS effort data."""
    log(f'Inserting {DATASET_ID} effort')
    cxn = db.connect()

    df = pd.read_csv(RAW_DIR / f'{EFFORT}.csv', dtype='unicode')
    df.to_sql('maps_effort', cxn, if_exists='replace', index=False)

    cxn.executescript("UPDATE maps_effort SET net = '?' WHERE net is NULL;")
    cxn.commit()


def insert_status():
    """Build a table to hold the raw MAPS effort data."""
    log(f'Inserting {DATASET_ID} status')
    cxn = db.connect()

    df = pd.read_csv(RAW_DIR / f'{STATUS}.csv', dtype='unicode')
    df.to_sql('maps_status', cxn, if_exists='replace', index=False)

    cxn.commit()


def insert_bands():
    """Build a table to hold the raw MAPS bands data."""
    log(f'Inserting {DATASET_ID} bands')
    cxn = db.connect()

    df = pd.read_csv(RAW_DIR / f'{BAND}.csv', dtype='unicode')
    df['count_id'] = db.create_ids(df, 'counts')

    df.to_sql('maps_bands', cxn, if_exists='replace', index=False)
    cxn.executescript("UPDATE maps_bands SET net = '?' WHERE net is NULL;")

    sql = """
        ALTER TABLE maps_bands ADD COLUMN YS TEXT;
        ALTER TABLE maps_bands ADD COLUMN year TEXT;

        UPDATE maps_bands SET year = SUBSTR(date, 1, 4);

        CREATE INDEX maps_bands_idx ON maps_bands (year, sta, spec);
        CREATE INDEX maps_status_idx ON maps_status (yr, sta, spec);

        UPDATE maps_bands AS b
           SET ys = (SELECT s.ys
                       FROM maps_status AS s
                      WHERE b.year = s.yr
                        AND b.sta  = s.sta
                        AND b.spec = s.spec);
        """
    with db.connect() as cxn:
        cxn.executescript(sql)
        cxn.commit()

    df = pd.read_sql('SELECT * FROM maps_bands;', cxn)

    df['count_json'] = json_object(df, """LOC BI BS PG C OBAND BAND SSN NUMB
        OSP SPEC OSP6 SPEC6 OA OHA AGE HA HA OWRP OWRP WRP OS OHS SEX HS SK CP
        BP F BM FM FW JP WNG WEIGHT STATUS DATE TIME STA STATION NET ANET DISP
        NOTE PPC SSC PPF SSF TT RR HD UPP UNP BPL NF FP SW COLOR SC CC BC MC
        WC JC OV1 V1 VM V94 V95 V96 V97 OVYR VYR N B A YS""".split())
    df.to_sql('maps_bands', cxn, if_exists='replace', index=False)


def insert_places():
    """
    Insert MAPS place data.

    This is a direct conversion of the MAPS place data (maps_stations) into
    the database's places table. We filter on valid data codes, latitudes,
    and longitudes. We also convert some fields like to place_json and
    precision to radius.
    """
    log(f'Inserting {DATASET_ID} places')

    sql = """
        INSERT INTO places
            (place_id, dataset_id, lng, lat, radius, place_json)
        SELECT place_id,
               ?                    AS dataset_id,
               CAST(declng AS REAL) AS lng,
               CAST(declat AS REAL) AS lat,
               CAST(CASE precision
                    WHEN '01S' THEN 30.92
                    WHEN '05S' THEN 30.92 * 5
                    WHEN '10S' THEN 30.92 * 10
                    WHEN '01M' THEN 111.32 * 1000
                    WHEN '10M' THEN 111.32 * 1000 * 10
                    WHEN 'BLK' THEN 111.32 * 1000 * 10
                    ELSE null
                    END AS REAL) AS radius,
               place_json
          FROM maps_stations
         WHERE LENGTH(sta) > 3
           AND lng BETWEEN -180.0 AND 180.0
           AND lat BETWEEN  -90.0 AND  90.0;
        """
    with db.connect() as cxn:
        cxn.execute(sql, (DATASET_ID, ))
        cxn.commit()


def insert_events():
    """Insert events."""
    log(f'Inserting {DATASET_ID} events')

    cxn = db.connect()

    # Group the events by STA, NET, and DATE
    sql = """
        SELECT
            MAX(place_id)                           AS place_id,
            CAST(STRFTIME('%Y', b.date) AS INTEGER) AS year,
            CAST(STRFTIME('%j', b.date) AS INTEGER) AS day,
            MIN(PRINTF('%s:%s', SUBSTR(COALESCE(e.start, '000'), 1, 2),
                SUBSTR(COALESCE(e.start, '000') || '00', 3, 2)))  AS started,
            MAX(PRINTF('%s:%s', SUBSTR(COALESCE(e.end, '000'), 1, 2),
                SUBSTR(COALESCE(e.end, '000')   || '00', 3, 2)))  AS ended,
            MAX(b.sta)                              AS STA,
            MAX(COALESCE(b.net, '?'))               AS NET,
            MAX(b.date)                             AS DATE,
            MAX(b.station)                          AS STATION,
            MAX(e.length)                           AS LENGTH
        FROM maps_bands  AS b
   LEFT JOIN maps_effort AS e
          ON b.sta = e.sta AND b.net = e.net AND b.date = e.date
        JOIN maps_stations AS s ON b.sta = s.sta
        JOIN places USING (place_id)
    GROUP BY b.sta, b.net, b.date;
        """
    df = pd.read_sql(sql, cxn)
    df['event_id'] = db.create_ids(df, 'events')
    df['dataset_id'] = DATASET_ID
    df['event_json'] = json_object(df, 'STA NET DATE STATION LENGTH'.split())
    df.loc[:, db.EVENT_FIELDS].to_sql(
        'events', cxn, if_exists='append', index=False)


def insert_counts():
    """
    Insert counts.

    Because we had to group the maps_effort records to make event records we
    have to try to link to the events records directly.
    """
    log(f'Inserting {DATASET_ID} counts')

    sql = """
        INSERT INTO counts
            (count_id, event_id, dataset_id, taxon_id, count, count_json)
        SELECT count_id,
               event_id,
               ?          AS dataset_id,
               taxon_id,
               1          AS count,
               count_json
          FROM maps_bands
          JOIN events
                ON sta  = JSON_EXTRACT(event_json, '$.STA')
               AND net  = JSON_EXTRACT(event_json, '$.NET')
               AND date = JSON_EXTRACT(event_json, '$.DATE')
          JOIN taxa USING (spec);
        """
    with db.connect() as cxn:
        cxn.execute(sql, (DATASET_ID, ))
        cxn.commit()


if __name__ == '__main__':
    ingest()
