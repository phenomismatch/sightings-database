"""Show example queries."""

from pathlib import Path
from datetime import datetime
import pandas as pd
import lib.sqlite as db


def queries():
    """Show example queries."""
    cxn = db.connect()
    now = datetime.now()

    report_name = f'sample_queries_{now.strftime("%Y-%m-%d")}.xlsx'
    report_path = Path('output') / report_name

    bbs_sql, bbs_df = _bbs_query(cxn)
    maps_sql, maps_df = _maps_query(cxn)
    ebird_sql, ebird_df = _ebird_query(cxn)
    global_sql, global_df = _global_query(cxn)

    queries = [
        {'dataset': 'Combined', 'query': global_sql},
        {'dataset': 'BBS', 'query': bbs_sql},
        {'dataset': 'MAPS', 'query': maps_sql},
        {'dataset': 'eBird', 'query': ebird_sql}]
    front_df = pd.DataFrame(queries)

    with pd.ExcelWriter(report_path) as writer:
        front_df.to_excel(writer, sheet_name='Queries', index=False)
        global_df.to_excel(writer, sheet_name='Combined', index=False)
        bbs_df.to_excel(writer, sheet_name='BBS', index=False)
        maps_df.to_excel(writer, sheet_name='MAPS', index=False)
        ebird_df.to_excel(writer, sheet_name='eBird', index=False)


def _bbs_query(cxn):
    sql = """
        SELECT *
          FROM dates
          JOIN bbs_events USING (date_id)
          JOIN counts     USING (date_id)
          JOIN bbs_counts USING (count_id)
          JOIN taxons     USING (taxon_id)
         WHERE year BETWEEN 2010 AND 2014
           AND day  BETWEEN  100 AND  200
           AND lng  BETWEEN  -79 AND  -78
           AND lat  BETWEEN   40 AND   44
         LIMIT 100"""
    df = pd.read_sql(sql, cxn)
    df.geopoint = 'BLOB'
    return sql, df


def _maps_query(cxn):
    sql = """
        SELECT *
          FROM dates
          JOIN maps_events USING (date_id)
          JOIN counts      USING (date_id)
          JOIN maps_counts USING (count_id)
          JOIN taxons      USING (taxon_id)
         WHERE year BETWEEN 2010 AND 2014
           AND day  BETWEEN  100 AND  200
           AND lng  BETWEEN  -79 AND  -78
           AND lat  BETWEEN   40 AND   44
         LIMIT 100"""
    df = pd.read_sql(sql, cxn)
    df.geopoint = 'BLOB'
    return sql, df


def _ebird_query(cxn):
    sql = """
        SELECT *
          FROM dates
          JOIN ebird_events USING (date_id)
          JOIN counts       USING (date_id)
          JOIN ebird_counts USING (count_id)
          JOIN taxons       USING (taxon_id)
         WHERE year BETWEEN 2010 AND 2014
           AND day  BETWEEN  100 AND  200
           AND lng  BETWEEN  -79 AND  -78
           AND lat  BETWEEN   40 AND   44
         LIMIT 100"""
    df = pd.read_sql(sql, cxn)
    df.geopoint = 'BLOB'
    return sql, df


def _global_query(cxn):
    sql = """
        SELECT *
          FROM dates
          JOIN counts USING (date_id)
          JOIN taxons USING (taxon_id)
         WHERE year BETWEEN 2010 AND 2014
           AND day  BETWEEN  100 AND  200
           AND lng  BETWEEN  -79 AND  -78
           AND lat  BETWEEN   40 AND   44
         LIMIT 100"""
    df = pd.read_sql(sql, cxn)
    df.geopoint = 'BLOB'
    return sql, df


if __name__ == '__main__':
    queries()
