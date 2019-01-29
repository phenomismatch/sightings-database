"""Some one-off reports."""

from datetime import datetime
import pandas as pd
import lib.db as db


def get_species(species):
    """Output data for the given species to the a CSV file."""
    species = ','.join([f"'{s}'" for s in species])

    sql = """SELECT taxa.*, places.*, events.*, counts.*
               FROM places
               JOIN events USING (place_id)
               JOIN counts USING (event_id)
               JOIN taxa USING (taxon_id)
              WHERE sci_name IN ({})""".format(species)
    return pd.read_sql(sql, db.connect())


if __name__ == '__main__':
    DF = get_species([
        'Papilio canadensis',
        'Anthocharis midea',
        'Plebejus saepiolus',
        'Glaucopsyche lygdamus',
        'Plebejus icarioides',
        'Speyeria mormonia',
        'Chlosyne palla',
        'Euphydryas editha',
        'Euphydryas phaeton',
        'Polygonia gracilis',
        'Nymphalis californica',
        'Cercyonis pegala',
        'Celastrina neglecta',
        'Speyeria atlantis',
        'Erynnis icelus',
        'Thymelicus lineola',
        'Ochlodes sylvanoides'])

    NOW = datetime.now()
    REPORT_NAME = f'temp/species_{NOW.strftime("%Y-%m-%d")}.csv'
    DF.to_csv(REPORT_NAME, index=False)
