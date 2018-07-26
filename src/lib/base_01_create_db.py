"""Common logic for creating the sightings database."""

from datetime import datetime
import pandas as pd
import lib.globals as g


class BaseCreateDb:
    """Create a sightings database and input constant data."""

    CLEMENTS_DATASET_ID = 'clements'

    def __init__(self, db):
        """Setup."""
        self.db = db
        self.cxn = None

    def create_database(self):
        """Create the database and input constant data."""
        self.db.create()

        self.cxn = self.db()

        self._insert_version()
        self._insert_countries()
        self._insert_datasets()
        self._insert_taxons()

    def _insert_version(self):
        print('Inserting version')
        df = pd.DataFrame({'version': ['v0.3'], 'created': datetime.now()})
        df.to_sql('version', self.cxn.engine, if_exists='replace')

    def _insert_countries(self):
        print('Inserting countries')

        path = str(g.EXTERNAL / 'misc' / 'ISO_3166-1_country_codes.csv')
        df = pd.read_csv(path).rename_axis('country_id')

        df.to_sql('countries', self.cxn.engine, if_exists='replace')
        self.cxn.execute('CREATE INDEX countries_code ON countries(code)')
        self.cxn.execute('CREATE INDEX countries_alpha2 ON countries(alpha2)')
        self.cxn.execute('CREATE INDEX countries_alpha3 ON countries(alpha3)')

    def _insert_taxons(self):
        print('Inserting taxons')
        self._insert_bird_taxons()
        # self.insert_lep_taxons()

    def _insert_bird_taxons(self):
        birds = self._get_clem_species()

        self._set_target_birds(birds)
        birds['genus'] = birds.sci_name.str.split().str[0]
        birds = self._add_clem_genera(birds)

        birds['dataset_id'] = self.CLEMENTS_DATASET_ID
        birds['class'] = 'aves'
        birds['synonyms'] = ''

        taxon_id = self.cxn.next_id('taxons')
        birds['taxon_id'] = range(taxon_id, taxon_id + birds.shape[0])
        birds = birds.rename(columns={'order': 'ordr'}).set_index('taxon_id')
        birds.to_sql('taxons', self.cxn.engine, if_exists='append')

    def _get_clem_species(self):
        path = str(g.TAXONOMY / 'Clements-Checklist-v2017-August-2017_2.csv')

        birds = pd.read_csv(path).rename(columns={
            'scientific name': 'sci_name', 'English name': 'common_name'})
        is_species = birds.category == 'species'
        birds = birds.loc[is_species,
                          ['sci_name', 'order', 'family', 'common_name']]
        return birds

    def _add_clem_genera(self, birds):
        targets = birds.loc[birds.target == 't']
        genera = targets.groupby('genus').first().reset_index()
        genera.sci_name = genera.genus + ' sp.'
        genera.common_name = ''
        genera.target = ''
        print(genera.shape)
        print(genera.head())
        return birds

    def _set_target_birds(self, birds):
        targets = pd.read_csv(
            g.TAXONOMY / 'target_birds.csv').sci_name.tolist()
        target = birds.sci_name.isin(targets)
        birds.loc[target, 'target'] = 't'

    def _insert_datasets(self):
        print('Inserting datasets')

        datasets = []

        datasets.append({
            'dataset_id': 'ISO 3166-1',
            'extracted': '2018-01-11',
            'version': '2018-01-11',
            'title': 'ISO 3166-1',
            'url': 'https://en.wikipedia.org/wiki/ISO_3166-1'})

        datasets.append({
            'dataset_id': self.CLEMENTS_DATASET_ID,
            'extracted': datetime.now(),
            'version': '2017-07-27',
            'title': 'Standardized birds species codes',
            'url': 'https://www.birdpop.org/pages/birdSpeciesCodes.php'})

        df = pd.DataFrame(datasets).set_index('dataset_id')
        df.to_sql('datasets', self.cxn.engine, if_exists='append')
