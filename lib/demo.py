"""Some one-off reports."""

from datetime import datetime
import pandas as pd
import lib.db as db


def get_species(species):
    """Output data for the given species to the a CSV file."""
    species = ','.join([f"'{s}'" for s in species])

    sql = """SELECT taxons.*, places.*, events.*, counts.*
               FROM places
               JOIN events USING (place_id)
               JOIN counts USING (event_id)
               JOIN taxons USING (taxon_id)
              WHERE sci_name IN ({})""".format(species)
    return pd.read_sql(sql, db.connect())


if __name__ == '__main__':
    df = get_species([
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

    now = datetime.now()
    report_name = f'temp/species_{now.strftime("%Y-%m-%d")}.csv'
    df.to_csv(report_name, index=False)
