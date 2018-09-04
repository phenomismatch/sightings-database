"""Some one-off reports."""

import pandas as pd
from lib.db_sqlite import DbSqlite


def three_butterflies():
    """Output data for three butterfly species."""
    cxn = DbSqlite()
    species = [
        'Erynnis horatius',
        'Asterocampa celtis',
        'Libytheana carinenta']
    species = ','.join([f"'{s}'" for s in species])

    sql = """SELECT taxons.*, places.*, events.*, counts.*
               FROM places
               JOIN events USING (place_id)
               JOIN counts USING (event_id)
               JOIN taxons USING (taxon_id)
              WHERE sci_name IN ({})""".format(species)
    data = pd.read_sql(sql, cxn.engine)
    data.geopoint = 'Geometry'
    data.to_csv('temp/three_butterflies.csv', index=False)


if __name__ == '__main__':
    three_butterflies()
