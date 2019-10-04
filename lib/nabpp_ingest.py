"""Ingest NABPP data."""

import re
import sys
import csv
from shutil import copy
from pathlib import Path
import pandas as pd


SRC_DIR = Path('data') / 'raw' / 'nabpp_in'
DST_DIR = Path('data') / 'raw' / 'nabpp_out'


SQ = "'"    # Single Quote
DQ = '"'    # Double Quote
ESQ = "''"  # Escaped Single Quote
SPLITTER = "','"    # Split single quoted fields


# The CSV files are heterogeneous, most use single quotes to surround a text
# field but a few use double quotes to surround text.
#   quotechar tells us which type of file: sinlge or double quoted text
#   cols = column count. This is used to check if the parsing
CSVS = [
    # {'file': 'test', 'quotechar': SQ, 'cols': 37},
    {'file': 'NABPP_2', 'quotechar': SQ, 'cols': 6},
    {'file': 'NABPP_adm1', 'quotechar': DQ, 'cols': 5},
    {'file': 'NABPP_cagns', 'quotechar': DQ, 'cols': 9},
    {'file': 'NABPP_discrepancy1', 'quotechar': SQ, 'cols': 18},
    {'file': 'NABPP_downloads', 'quotechar': SQ, 'cols': 5},
    {'file': 'NABPP_gnis', 'quotechar': SQ, 'cols': 19},
    {'file': 'NABPP_location1', 'quotechar': SQ, 'cols': 7},
    {'file': 'NABPP_obs_alias', 'quotechar': SQ, 'cols': 2},
    {'file': 'NABPP_observations', 'quotechar': SQ, 'cols': 37},
    {'file': 'NABPP_observer', 'quotechar': SQ, 'cols': 12},
    {'file': 'NABPP_rectifiable', 'quotechar': SQ, 'cols': 3},
    {'file': 'NABPP_rect_import', 'quotechar': SQ, 'cols': 26},
    {'file': 'NABPP_rect_listtest', 'quotechar': SQ, 'cols': 1},
    {'file': 'NABPP_rect_test', 'quotechar': SQ, 'cols': 5},
    {'file': 'NABPP_species_matched', 'quotechar': DQ, 'cols': 9},
    {'file': 'NABPP_species', 'quotechar': DQ, 'cols': 4},
    {'file': 'NABPP_transcriber', 'quotechar': SQ, 'cols': 20},
    {'file': 'NABPP_transcription', 'quotechar': SQ, 'cols': 26},
    {'file': 'NABPP_validatable', 'quotechar': SQ, 'cols': 3},
    {'file': 'NABPP_validated_obs_vw', 'quotechar': DQ, 'cols': 3},
    ]


def reformat_csvs():
    """Reformatting the CSVs."""
    for table in CSVS:
        print('Reformatting', table['file'])
        _, path = get_file_names(table)
        df = pd.read_csv(
            path,
            quotechar=table['quotechar'],
            dtype=object,       # Silences but doesn't fix warnings
            encoding='latin-1')
        df.to_csv(path, index=False)


def get_file_names(table):
    """Get the source and destination file name."""
    src = SRC_DIR / (table['file'] + '.txt')
    dst = DST_DIR / (table['file'] + '.csv')
    return src, dst


def fix_files():
    """Fix files."""
    for table in CSVS:
        print('Fixing', table['file'])

        src, dst = get_file_names(table)

        if table['quotechar'] == SQ:
            fix_single_quotes(table, src, dst)
        else:
            copy(src, dst)


def check_files():
    """Check that converted files."""
    for table in CSVS:
        print('Checking', table['file'])
        check_with_csv(table)
        check_with_pandas(table)


def check_with_pandas(table):
    """Pandas finds more errors."""
    _, path = get_file_names(table)
    pd.read_csv(
        path,
        quotechar=table['quotechar'],
        dtype=object,       # Silences but doesn't fix warnings
        encoding='latin-1')


def check_with_csv(table):
    """CSV gives more information about where errors are in the file."""
    _, path = get_file_names(table)
    with open(path, encoding='latin-1') as in_file:
        reader = csv.DictReader(in_file, quotechar=table['quotechar'])
        for row in reader:
            if len(row) != table['cols']:
                for key, value in row.items():
                    if key:
                        print(f'{key}\t\t{value}')
                print(f'Columns: {len(row)}')
                sys.exit()


def fix_single_quotes(table, src, dst):
    """
    Fix missing escaped single quotes building new properly escaped files.

    The biggest problem with the data is the data is that single quotes are
    used both to surround text data and as normal single quotes. The
    interior single quotes have to be escaped by doubling them. So, ' will
    become ''. The field exterior quotes should be left alone.

    One wrinkle is that there are multi-line fields which means that we can't
    just treat all quotes at the start (or end) of a line as exterior quotes.
    We count the fields in the line to determine if we're at the start or end
    of a row.
    """
    cols = table['cols']

    with open(src, encoding='latin-1') as in_file, open(dst, 'w') as out_file:

        column = 0

        for line in in_file:
            fields = line.split(SPLITTER)

            new = []
            for field in fields:
                new.append(field.replace(SQ, ESQ))

            # The first quote (first char) in a row should not be escaped
            if column % cols == 0:
                new[0] = re.sub(fr"^{ESQ}", SQ, new[0])

            # Transition from first to last column
            column += len(fields)
            column -= 0 if column % cols == 0 else 1  # Handle multi-line cell

            # The last quote (last char) in a row should not be escaped
            if column % cols == 0:
                new[-1] = re.sub(fr"{ESQ}(\s*)$", fr"{SQ}\1", new[-1])

            line = SPLITTER.join(new)
            out_file.write(line)


def get_metadata():
    """Build raw metadata for the tables."""
    rows = []
    for table in CSVS:
        print('Metadata', table['file'])
        _, path = get_file_names(table)
        with open(path, encoding='latin-1') as in_file:
            reader = csv.reader(in_file)
            header = next(reader)
        rows += [{'table': table['file'],
                  'column': '',
                  'primary_key': '',
                  'foreign_key1': '',
                  'foreign_key2': '',
                  'foreign_key3': ''}]
        cols = [{'table': table['file'], 'column': c} for c in header]
        rows += cols

    df = pd.DataFrame(rows)

    df.to_csv(DST_DIR / 'metadata.csv', index=False)


if __name__ == '__main__':
    # fix_files()
    # check_files()
    # reformat_csvs()
    get_metadata()
