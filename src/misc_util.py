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

    species = [f"'{s}'" for s in species]
    sql = f"select * from taxons where sci_name in ({','.join(species)})"
    taxons = pd.read_sql(sql, cxn.engine)
    taxon_ids = ','.join(taxons.taxon_id.astype(str).tolist())

    sql = """select places.*
               from places
               join events using (place_id)
               join counts using (event_id)
              where taxon_id in ({})""".format(taxon_ids)
    places = pd.read_sql(sql, cxn.engine)
    places.geopoint = 'Geometry'
    sql = """select naba_places.*
               from naba_places
               join events using (place_id)
               join counts using (event_id)
              where taxon_id in ({})""".format(taxon_ids)
    naba_places = pd.read_sql(sql, cxn.engine)
    sql = """select pollard_places.*
               from pollard_places
               join events using (place_id)
               join counts using (event_id)
              where taxon_id in ({})""".format(taxon_ids)
    pollard_places = pd.read_sql(sql, cxn.engine)

    sql = """select events.*
               from events
               join counts using (event_id)
              where taxon_id in ({})""".format(taxon_ids)
    events = pd.read_sql(sql, cxn.engine)
    sql = """select naba_events.*
               from naba_events
               join counts using (event_id)
              where taxon_id in ({})""".format(taxon_ids)
    naba_events = pd.read_sql(sql, cxn.engine)
    sql = """select pollard_events.*
               from pollard_events
               join counts using (event_id)
              where taxon_id in ({})""".format(taxon_ids)
    pollard_events = pd.read_sql(sql, cxn.engine)

    sql = """select counts.*
               from counts
              where taxon_id in ({})""".format(taxon_ids)
    counts = pd.read_sql(sql, cxn.engine)
    sql = """select naba_counts.*
               from naba_counts
               join counts using (count_id)
              where taxon_id in ({})""".format(taxon_ids)
    naba_counts = pd.read_sql(sql, cxn.engine)
    sql = """select pollard_counts.*
               from pollard_counts
               join counts using (count_id)
              where taxon_id in ({})""".format(taxon_ids)
    pollard_counts = pd.read_sql(sql, cxn.engine)

    with pd.ExcelWriter('temp/three_butterflies.xlsx') as writer:
        taxons.to_excel(writer, sheet_name='taxons')
        places.to_excel(writer, sheet_name='places')
        events.to_excel(writer, sheet_name='events')
        counts.to_excel(writer, sheet_name='counts')
        naba_places.to_excel(writer, sheet_name='naba_places')
        naba_events.to_excel(writer, sheet_name='naba_events')
        naba_counts.to_excel(writer, sheet_name='naba_counts')
        pollard_places.to_excel(writer, sheet_name='pollard_places')
        pollard_events.to_excel(writer, sheet_name='pollard_events')
        pollard_counts.to_excel(writer, sheet_name='pollard_counts')


if __name__ == '__main__':
    three_butterflies()
