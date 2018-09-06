"""Global constants."""

from pathlib import Path


CLEMENTS_DATASET_ID = 'clements'
POLLARD_DATASET_ID = 'pollard'
NABA_DATSET_ID = 'naba'

DATA_DIR = Path('data')
EXTERNAL = DATA_DIR / 'external'
INTERIM = DATA_DIR / 'interim'
TEMP = DATA_DIR / 'temp'
TAXONOMY = EXTERNAL / 'taxonomy'

NABA_PATH = DATA_DIR / 'raw' / NABA_DATSET_ID
POLLARD_PATH = DATA_DIR / 'raw' / POLLARD_DATASET_ID
