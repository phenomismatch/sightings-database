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
    # {'file': 'test.txt', 'quotechar': SQ, 'cols': 37},
    {'file': 'NABPP_2.txt', 'quotechar': SQ, 'cols': 6},
    {'file': 'NABPP_adm1.txt', 'quotechar': DQ, 'cols': 5},
    {'file': 'NABPP_cagns.txt', 'quotechar': DQ, 'cols': 9},
    {'file': 'NABPP_discrepancy1.txt', 'quotechar': SQ, 'cols': 18},
    {'file': 'NABPP_downloads.txt', 'quotechar': SQ, 'cols': 5},
    {'file': 'NABPP_gnis.txt', 'quotechar': SQ, 'cols': 19},
    {'file': 'NABPP_location1.txt', 'quotechar': SQ, 'cols': 7},
    {'file': 'NABPP_obs_alias.txt', 'quotechar': SQ, 'cols': 2},
    {'file': 'NABPP_observations.txt', 'quotechar': SQ, 'cols': 37},
    {'file': 'NABPP_observer.txt', 'quotechar': SQ, 'cols': 12},
    {'file': 'NABPP_rectifiable.txt', 'quotechar': SQ, 'cols': 3},
    {'file': 'NABPP_rect_import.txt', 'quotechar': SQ, 'cols': 26},
    {'file': 'NABPP_rect_listtest.txt', 'quotechar': SQ, 'cols': 1},
    {'file': 'NABPP_rect_test.txt', 'quotechar': SQ, 'cols': 5},
    {'file': 'NABPP_species_matched.txt', 'quotechar': DQ, 'cols': 9},
    {'file': 'NABPP_species.txt', 'quotechar': DQ, 'cols': 4},
    {'file': 'NABPP_transcriber.txt', 'quotechar': SQ, 'cols': 20},
    {'file': 'NABPP_transcription.txt', 'quotechar': SQ, 'cols': 26},
    {'file': 'NABPP_validatable.txt', 'quotechar': SQ, 'cols': 3},
    {'file': 'NABPP_validated_obs_vw.txt', 'quotechar': DQ, 'cols': 3},
    ]


def fix_files():
    """Fix files."""
    for table in CSVS:
        print('Fixing', table['file'])

        src = SRC_DIR / table['file']
        dst = DST_DIR / (table['file'][:-3] + 'csv')
        table['file'] = dst

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
    pd.read_csv(
        table['file'],
        quotechar=table['quotechar'],
        dtype=object,       # Silences but doesn't fix warnings
        encoding='latin-1')


def check_with_csv(table):
    """CSV gives more information about where errors are in the file."""
    with open(table['file'], encoding='latin-1') as in_file:
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
    just treat all quotes at the start (or end) of a line as a exterior quotes.
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


if __name__ == '__main__':
    fix_files()
    check_files()
