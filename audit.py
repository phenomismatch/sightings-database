"""Run some sanity checks on the DB."""

import csv
import gzip
from pathlib import Path
from datetime import datetime


def ebird():
    """Verify that the ebird records seem correct."""
    csv_path = Path('data') / 'raw' / 'ebird' / 'ebd_relDec-2018.txt.gz'
    target_path = Path('data') / 'raw' / 'taxonomy' / 'target_birds.csv'

    targets = {}
    with open(target_path) as target_file:
        reader = csv.DictReader(target_file)
        for row in reader:
            targets[row['sci_name']] = 0

    places = set()
    events = set()

    with gzip.open(csv_path, 'rt') as ebird_file:
        reader = csv.DictReader(ebird_file, delimiter='\t')
        i = 0
        while True:
            i += 1
            if i % 1_000_000 == 0:
                print(i)
            try:
                row = next(reader)
            except csv.Error as err:
                print(err)
                continue
            except StopIteration:
                break

            if row['APPROVED'] != '1' or row['ALL SPECIES REPORTED'] != '1':
                continue
            try:
                datetime.strptime(row['OBSERVATION DATE'], '%Y-%m-%d')
                lng = float(row['LONGITUDE'])
                lat = float(row['LATITUDE'])
            except ValueError:
                continue

            if lng < -95.0 or lng > -50.0 or lat < 20.0:
                continue

            places.add((lng, lat))
            events.add(row['SAMPLING EVENT IDENTIFIER'])

            if row['SCIENTIFIC NAME'] not in targets:
                continue

            targets[row['SCIENTIFIC NAME']] += 1

    counts = 0
    for key in sorted(targets.keys()):
        counts += targets[key]
        print(key, targets[key])
    print('places', len(places))
    print('events', len(events))
    print('counts', counts)


if __name__ == '__main__':
    ebird()
