"""Common logic for creating the sightings database."""

from datetime import datetime
import pandas as pd
import lib.data as data
from lib.util import Clements, Pollard, Naba, Countries, TargetBirds
from lib.util import Caterpillar


class CreateDb:
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
        sql = 'INSERT INTO version (version, created) VALUES (?, ?)'
        self.cxn.execute(sql, values=('v0.4',  datetime.now()))

    def _insert_countries(self):
        print('Inserting countries')
        path = str(Countries.csv)
        df = pd.read_csv(path).set_index('code')
        df.to_sql('countries', self.cxn.engine, if_exists='replace')

    def _insert_taxons(self):
        print('Inserting taxons')
        self._insert_bird_taxons()
        self._insert_lep_taxons()
        self._insert_caterpillar_taxons()

    def _insert_lep_taxons(self):
        pollard = self._get_pollard_taxons()
        naba = self._get_naba_taxons()
        taxons = pd.concat([pollard, naba], sort=True)

        taxons.sci_name = taxons.sci_name.str.split().str.join(' ')
        taxons = taxons.drop_duplicates('sci_name')

        taxons['class'] = 'lepidoptera'
        taxons['order'] = ''
        taxons['group'] = None
        taxons['family'] = ''
        taxons['target'] = 't'

        taxons = self.cxn.add_taxon_id(taxons)
        self.cxn.insert_taxons(taxons)

    def _get_pollard_taxons(self):
        taxons = pd.read_csv(Pollard.data_csv, dtype='unicode')
        taxons = taxons.rename(columns={
            'Scientific Name': 'sci_name',
            'Species': 'common_name'})
        taxons = taxons.loc[taxons.sci_name.notna(),
                            ['sci_name', 'common_name']]
        taxons = taxons.drop_duplicates('sci_name')
        taxons['taxon_dataset_id'] = Pollard.dataset_id
        taxons['genus'] = taxons.sci_name.str.split().str[0]
        return taxons

    def _get_naba_taxons(self):
        taxons = pd.read_csv(Naba.csv, dtype='unicode')
        taxons = taxons.rename(columns={
            'Gen/Tribe/Fam': 'genus', 'Species': 'species'})
        taxons['sci_name'] = taxons.apply(
            lambda x: f'{x.genus} {x.species}', axis='columns')
        taxons = taxons.drop_duplicates('sci_name')
        taxons = taxons.loc[:, ['sci_name', 'genus']].copy()
        taxons['taxon_dataset_id'] = Naba.dataset_id
        taxons['common_name'] = ''
        return taxons

    def _insert_bird_taxons(self):
        taxons = self._get_clem_species()

        self._set_target_birds(taxons)
        taxons['genus'] = taxons.sci_name.str.split().str[0]
        taxons = data.add_taxon_genera_records(taxons)

        taxons['class'] = 'aves'
        taxons['group'] = None

        taxons = self.cxn.add_taxon_id(taxons)
        self.cxn.insert_taxons(taxons)

    def _get_clem_species(self):
        taxons = pd.read_csv(Clements.csv, dtype='unicode').rename(columns={
            'scientific name': 'sci_name', 'English name': 'common_name'})
        is_species = taxons.category == 'species'
        taxons = taxons.loc[is_species,
                            ['sci_name', 'order', 'family', 'common_name']]
        taxons['taxon_dataset_id'] = Clements.dataset_id
        return taxons

    def _set_target_birds(self, taxons):
        targets = pd.read_csv(TargetBirds.csv).sci_name.tolist()
        target = taxons.sci_name.isin(targets)
        taxons.loc[target, 'target'] = 't'

    def _insert_caterpillar_taxons(self):
        taxons = pd.read_csv(Caterpillar.sightings_csv, dtype='unicode')
        taxons = taxons.rename(columns={'Group': 'group'})
        taxons = taxons.drop_duplicates('group')
        taxons['taxon_dataset_id'] = Caterpillar.dataset_id
        taxons['class'] = None
        taxons['order'] = None
        taxons['genus'] = None
        taxons['family'] = None
        taxons['target'] = None
        taxons['sci_name'] = None
        taxons['common_name'] = None
        taxons = self.cxn.add_taxon_id(taxons)
        self.cxn.insert_taxons(taxons)

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
            'dataset_id': Clements.dataset_id,
            'extracted': datetime.now(),
            'version': '2017-07-27',
            'title': 'Standardized birds species codes',
            'url': 'https://www.birdpop.org/pages/birdSpeciesCodes.php'})

        df = pd.DataFrame(datasets).set_index('dataset_id')
        df.to_sql('datasets', self.cxn.engine, if_exists='append')


class CreateDbPostgres(CreateDb):
    """Create a postgres database."""


class CreateDbSqlite(CreateDb):
    """Create a sqlite database."""
