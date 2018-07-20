"""Ingest eBird data."""

from datetime import date
import warnings
import pandas as pd
import geohash2
import lib.sqlite as db
import lib.data as data


DATASET_ID = 'ebird'
EBIRD_PATH = db.DATA_DIR / 'raw' / DATASET_ID


def ingest_ebird():
    """Ingest ebird data."""
    warnings.simplefilter(action='ignore', category=UserWarning)

    cxn = db.connect()

    db.delete_dataset(cxn, DATASET_ID)
    insert_dataset(cxn)
    taxons = select_taxons(cxn)
    insert_codes(cxn)
    insert_records(cxn, taxons)
    db.update_points(cxn, DATASET_ID)


def insert_dataset(cxn):
    """Insert a dataset record."""
    print('Inserting dataset')

    dataset = dict(
        dataset_id=DATASET_ID,
        title='eBird Basic Dataset',
        extracted=str(date.today()),
        version='relFeb-2018',
        url='https://ebird.org/home')
    db.insert_dataset(cxn, dataset)


def select_taxons(cxn):
    """Build a dictionary of scientific names and taxon_ids."""
    print('Selecting taxons')

    sql = """SELECT taxon_id, sci_name FROM taxons WHERE target = 1"""
    taxons = pd.read_sql(sql, cxn)
    return taxons.set_index('sci_name').taxon_id.to_dict()


def insert_records(cxn, taxons):
    """Insert event and event detail records."""
    print('Inserting dates and counts')

    in_path = EBIRD_PATH / 'ebd_relFeb-2018.txt'

    rename_columns = {
        'OBSERVATION COUNT': 'count',
        'EFFORT DISTANCE KM': 'radius',
        'TIME OBSERVATIONS STARTED': 'started',
        'LATITUDE': 'lat',
        'LONGITUDE': 'lng'}
    event_cols = db.EVENT_COLUMNS + [
        'COUNTRY CODE', 'STATE CODE', 'COUNTY CODE', 'IBA CODE', 'BCR CODE',
        'USFWS CODE', 'ATLAS BLOCK', 'LOCALITY ID', ' LOCALITY TYPE',
        'SAMPLING EVENT IDENTIFIER', 'PROTOCOL TYPE', 'PROTOCOL CODE',
        'PROJECT CODE', 'DURATION MINUTES', 'EFFORT AREA HA', 'APPROVED',
        'REVIEWED', 'NUMBER OBSERVERS', 'ALL SPECIES REPORTED',
        'OBSERVATION DATE', 'GROUP IDENTIFIER']
    count_cols = db.COUNT_COLUMNS + [
        'GLOBAL UNIQUE IDENTIFIER', 'LAST EDITED DATE', 'TAXONOMIC ORDER',
        'CATEGORY', 'SUBSPECIES SCIENTIFIC NAME', 'BREEDING BIRD ATLAS CODE',
        'BREEDING BIRD ATLAS CATEGORY', 'AGE/SEX', 'OBSERVER ID', 'HAS MEDIA']
    sample_ids = {}

    chunk = 1_000_000
    reader = pd.read_csv(
        in_path, delimiter='\t', quoting=3, chunksize=chunk, dtype='unicode')

    for i, df in enumerate(reader, 1):
        print(f'chunk {i * chunk:,}')

        df = df.rename(columns=rename_columns)

        df['event_date'] = pd.to_datetime(
            df['OBSERVATION DATE'], errors='coerce')

        has_date = df.event_date.notna()
        has_count = pd.to_numeric(df['count'], errors='coerce').notna()
        is_approved = df.APPROVED == '1'
        is_complete = df['ALL SPECIES REPORTED'] == '1'
        in_species = df['SCIENTIFIC NAME'].isin(taxons)
        df = df.loc[
            has_date & has_count & is_approved & is_complete & in_species, :]
        df = data.filter_lat_lng(df, lat=(20.0, 90.0), lng=(-95.0, -50.0))

        if df.shape[0] == 0:
            continue

        df['count'] = df['count'].apply(int)
        df['year'] = df.event_date.dt.strftime('%Y')
        df['day'] = df.event_date.dt.strftime('%j')
        df['taxon_id'] = df['SCIENTIFIC NAME'].map(taxons)
        df['dataset_id'] = DATASET_ID

        df['started'] = pd.to_datetime(df['started'], format='%H:%M:%S')
        df['delta'] = pd.to_numeric(df['DURATION MINUTES'], errors='coerce')
        df.delta = pd.to_timedelta(df.delta, unit='m', errors='coerce')
        df['ended'] = df.started + df.delta
        convert_to_time(df, 'started')
        convert_to_time(df, 'ended')

        is_na = df.radius.isna()
        df.radius = pd.to_numeric(df.radius, errors='coerce').fillna(0.0)
        df.radius *= 1000.0
        df.loc[is_na, 'radius'] = None

        dups = df['SAMPLING EVENT IDENTIFIER'].duplicated()
        dates = df[~dups]
        old_events = dates['SAMPLING EVENT IDENTIFIER'].isin(sample_ids)
        dates = dates[~old_events]

        date_id = db.next_id(cxn, 'dates')
        dates['date_id'] = range(date_id, date_id + dates.shape[0])
        dates['geohash'] = dates.apply(lambda x: geohash2.encode(
            x.lat, x.lng, precision=7), axis=1)

        dates = dates.set_index('SAMPLING EVENT IDENTIFIER', drop=False)
        new_sample_ids = dates.date_id.to_dict()
        sample_ids = {**sample_ids, **new_sample_ids}

        dates = dates.set_index('date_id')

        data.insert_events(dates.loc[:, event_cols], cxn, 'ebird_events')

        df['date_id'] = df['SAMPLING EVENT IDENTIFIER'].map(sample_ids)
        df = data.add_count_id(df, cxn)

        data.insert_counts(df.loc[:, count_cols], cxn, 'ebird_counts')


def convert_to_time(df, column):
    """Convert the time field from datetime format to HH:MM format."""
    is_na = df[column].isna()
    df[column] = df[column].dt.strftime('%H:%M')
    df.loc[is_na, column] = None


def insert_codes(cxn):
    """Insert eBird code values into the database."""
    print('Inserting codes')

    bcr = pd.read_csv(
        EBIRD_PATH / 'BCRCodes.txt', sep='\t', encoding='ISO-8859-1')
    bcr['field'] = 'BCR CODE'

    iba = pd.read_csv(
        EBIRD_PATH / 'IBACodes.txt', sep='\t', encoding='ISO-8859-1')
    iba['field'] = 'IBA CODE'

    usfws = pd.read_csv(
        EBIRD_PATH / 'USFWSCodes.txt', sep='\t', encoding='ISO-8859-1')
    usfws['field'] = 'USFWS CODE'

    codes = pd.read_csv(EBIRD_PATH / 'ebird_codes.csv')
    df = codes.append([bcr, iba, usfws], ignore_index=True, sort=True)

    df.to_sql('ebird_codes', cxn, if_exists='replace', index=False)


if __name__ == '__main__':
    ingest_ebird()
