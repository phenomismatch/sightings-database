"""Some one-off reports."""

import pandas as pd
from lib.sqlite_db import SqliteDb


def three_butterflies():
    """Output data for three butterfly species."""
    cxn = SqliteDb()
    species = [
        'Erynnis horatius',
        'Asterocampa celtis',
        'Libytheana carinenta']
    species = ','.join([f"'{s}'" for s in species])

    sql = """select taxons.*, places.*, events.*, counts.*
               from places
               join events using (place_id)
               join counts using (event_id)
               join taxons using (taxon_id)
              where sci_name in ({})""".format(species)
    data = pd.read_sql(sql, cxn.engine)
    data.geopoint = 'Geometry'
    data.to_csv('temp/three_butterflies.csv', index=False)


if __name__ == '__main__':
    three_butterflies()
