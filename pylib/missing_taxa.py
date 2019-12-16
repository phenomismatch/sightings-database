"""Find the bird taxons that are missing from Clements taxonomy."""

from pathlib import Path
import pandas as pd
from . import db


OUTPUT_CSV = Path('output') / 'missing_taxa.csv'
TARGET_CSV = Path('data') / 'raw' / 'taxonomy' / 'target_birds.csv'
BBS_CSV = Path('data') / 'raw' / 'bbs' / 'breed_bird_survey_species.csv'
MAPS_CSV = Path('data') / 'raw' / 'maps' / 'LIST17.csv'
COLUMNS = 'dataset sci_name common_name key'.split()


def missing_birds():
    """Find scientific names that are not in Clements taxonomy."""
    taxa = pd.DataFrame(columns=COLUMNS)
    taxa.to_csv(OUTPUT_CSV, index=False)
    missing_targets()
    missing_bbs()
    missing_maps()


def missing_targets():
    """Find target birds missing from Clements taxonomy."""
    taxa = pd.read_csv(TARGET_CSV)
    taxa['dataset'] = 'target birds'
    taxa['key'] = ''
    taxa = db.drop_duplicate_taxa(taxa)
    taxa.loc[:, COLUMNS].to_csv(
        OUTPUT_CSV, mode='a', index=False, header=False)


def missing_bbs():
    """Find bbs birds missing from Clements taxonomy."""
    taxa = pd.read_csv(BBS_CSV)
    taxa['sci_name'] = taxa.genus + ' ' + taxa.species
    taxa['common_name'] = taxa.english_common_name
    taxa['dataset'] = 'bbs'
    taxa['key'] = taxa.aou.apply(lambda x: f'aou = {x}')
    taxa = db.drop_duplicate_taxa(taxa)
    taxa.loc[:, COLUMNS].to_csv(
        OUTPUT_CSV, mode='a', index=False, header=False)


def missing_maps():
    """Find maps birds missing from Clements taxonomy."""
    taxa = pd.read_csv(MAPS_CSV)
    taxa['sci_name'] = taxa.SCINAME
    taxa['common_name'] = taxa.COMMONNAME
    taxa['dataset'] = 'maps'
    taxa['key'] = taxa.SPEC.apply(lambda x: f'SPEC = {x}')
    taxa = db.drop_duplicate_taxa(taxa)
    taxa.loc[:, COLUMNS].to_csv(
        OUTPUT_CSV, mode='a', index=False, header=False)


if __name__ == '__main__':
    missing_birds()
