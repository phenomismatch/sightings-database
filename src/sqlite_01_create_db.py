"""Create the SQLite3 database."""

# import pandas as pd
# import lib.sqlite as db
from lib.base_create_db import BaseCreateDb
from lib.sqlite import Connection
# import lib.globals as g


class SqliteCreateDb(BaseCreateDb):
    """Create the sqlite database and input constant data."""
# CLEMENTS_DATASET_ID = 'clements'
# EXTERNAL = db.DATA_DIR / 'external'
# TAXONOMY = EXTERNAL / 'taxonomy'


# def _insert_datasets(cxn, datasets):
#     print('Inserting datasets')
#     df = pd.DataFrame(datasets)
#     df = df.set_index('dataset_id')
#     df = df.to_sql('datasets', cxn, if_exists='append')
#
#
# def _insert_taxons(cxn, datasets):
#     print('Inserting taxons')
#     _insert_bird_taxons(cxn, datasets)
#     # insert_lep_taxons(cxn, datasets)
#
#
# def _insert_bird_taxons(cxn, datasets):
#     birds = _get_clem_species(datasets)
#     birds['dataset_id'] = CLEMENTS_DATASET_ID
#     birds['genus'] = birds.sci_name.str.split().str[0]
#     birds['class'] = 'aves'
#     birds['synonyms'] = ''
#
#     taxon_id = db.next_id(cxn, 'taxons')
#     birds['taxon_id'] = range(taxon_id, taxon_id + birds.shape[0])
#
#     birds = birds.rename(columns={'order': 'ordr'}).set_index('taxon_id')
#     _set_target_birds(birds)
#     birds.to_sql('taxons', cxn, if_exists='append')
#
#
# def _get_clem_species(datasets):
#     path = str(TAXONOMY / 'Clements-Checklist-v2017-August-2017_2.csv')
#
#     datasets.append({
#         'dataset_id': CLEMENTS_DATASET_ID,
#         'extracted': str(date.today()),
#         'version': '2017-07-27',
#         'title': 'Standardized birds species codes',
#         'url': 'https://www.birdpop.org/pages/birdSpeciesCodes.php'})
#
#     birds = pd.read_csv(path).rename(columns={'scientific name': 'sci_name',
#                                               'English name': 'common_name'})
#     is_species = birds.category == 'species'
#     birds = birds.loc[is_species,
#                       ['sci_name', 'order', 'family', 'common_name']]
#     return birds
#
#
# def _set_target_birds(birds):
#     targets = pd.read_csv(TAXONOMY / 'target_birds.csv').sci_name.tolist()
#     target = birds.sci_name.isin(targets)
#     birds.loc[target, 'target'] = 1


if __name__ == '__main__':
    create_database()
