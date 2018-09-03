"""Common logic for creating the sightings database."""

from datetime import datetime
import pandas as pd
import lib.data as data
import lib.globals as g


class BaseCreateDb:
    """Create a sightings database and input constant data."""

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
        df = pd.DataFrame({'version': ['v0.4'], 'created': datetime.now()})
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
        self._insert_lep_taxons()

    def _insert_lep_taxons(self):
        pollard = self._get_pollard_taxons()
        naba = self._get_naba_taxons()
        taxons = pd.concat([pollard, naba], sort=True)

        taxons.sci_name = taxons.sci_name.str.split().str.join(' ')
        taxons = taxons.drop_duplicates('sci_name')

        taxons['class'] = 'lepidoptera'
        taxons['order'] = ''
        taxons['family'] = ''
        taxons['target'] = 't'

        taxons = self.cxn.add_taxon_id(taxons)
        self.cxn.insert_taxons(taxons)

    def _get_pollard_taxons(self):
        taxons = pd.read_csv(
            g.POLLARD_PATH / 'pollardbase_example_201802.csv',
            dtype='unicode')
        taxons = taxons.rename(columns={
            'Scientific Name': 'sci_name',
            'Species': 'common_name'})
        taxons = taxons.loc[taxons.sci_name.notna(),
                            ['sci_name', 'common_name']].copy()
        taxons['dataset_id'] = g.POLLARD_DATASET_ID
        taxons['genus'] = taxons.sci_name.str.split().str[0]
        return taxons

    def _get_naba_taxons(self):
        taxons = pd.read_csv(
            g.NABA_PATH / 'NABA_JULY4.csv', dtype='unicode')
        taxons = taxons.rename(columns={
            'Gen/Tribe/Fam': 'genus',
            'Species_Epithet': 'species'})
        taxons['sci_name'] = taxons.apply(
            lambda x: f'{x.genus} {x.species}', axis='columns')
        taxons = taxons.loc[:, ['sci_name', 'genus']].copy()
        taxons['dataset_id'] = g.NABA_DATSET_ID
        taxons['common_name'] = ''
        return taxons

    def _insert_bird_taxons(self):
        taxons = self._get_clem_species()

        self._set_target_birds(taxons)
        taxons['genus'] = taxons.sci_name.str.split().str[0]
        taxons = data.add_taxon_genera_records(taxons)

        taxons['dataset_id'] = g.CLEMENTS_DATASET_ID
        taxons['class'] = 'aves'

        taxons = self.cxn.add_taxon_id(taxons)
        taxons = taxons.rename(columns={'order': 'order'})
        taxons['dataset_id'] = g.POLLARD_DATASET_ID
        self.cxn.insert_taxons(taxons)

    def _get_clem_species(self):
        path = str(g.TAXONOMY / 'Clements-Checklist-v2017-August-2017_2.csv')

        taxons = pd.read_csv(path).rename(columns={
            'scientific name': 'sci_name', 'English name': 'common_name'})
        is_species = taxons.category == 'species'
        taxons = taxons.loc[is_species,
                            ['sci_name', 'order', 'family', 'common_name']]
        return taxons

    def _set_target_birds(self, taxons):
        targets = pd.read_csv(
            g.TAXONOMY / 'target_birds.csv').sci_name.tolist()
        target = taxons.sci_name.isin(targets)
        taxons.loc[target, 'target'] = 't'

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
            'dataset_id': g.CLEMENTS_DATASET_ID,
            'extracted': datetime.now(),
            'version': '2017-07-27',
            'title': 'Standardized birds species codes',
            'url': 'https://www.birdpop.org/pages/birdSpeciesCodes.php'})

        df = pd.DataFrame(datasets).set_index('dataset_id')
        df.to_sql('datasets', self.cxn.engine, if_exists='append')
