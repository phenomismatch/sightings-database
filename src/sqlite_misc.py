"""Create data samples of various queries on the database."""

from datetime import datetime
import pandas as pd
import lib.sqlite as db


def generate_samples():
    """Generate excel sheets."""
    ecols = [db.EVENT_INDEX]
    ecols += ['events.' + c for c in db.EVENT_COLUMNS]
    ecols += ["'geometry' AS point"]
    ecols = ', '.join(ecols)

    limit = 'limit 50'
    eusing = f'USING (event_id) {limit}'
    cusing = f'USING (count_id) {limit}'

    queries = [
        ('Taxons', f'SELECT * FROM taxons {limit}'),
        ('Events only', f'SELECT {ecols} FROM events {limit}'),
        ('Counts only', f'SELECT * FROM counts {limit}'),
        ('BBS events',
         (f'SELECT {ecols}, bbs_events.* '
          f'FROM events JOIN bbs_events {eusing}')),
        ('MAPS events',
         (f'SELECT {ecols}, maps_events.* '
          f'FROM events JOIN maps_events {eusing}')),
        ('eBird events',
         (f'SELECT {ecols}, ebird_events.* '
          f'FROM events JOIN ebird_events {eusing}')),
        ('BBS counts', f'SELECT * FROM counts JOIN bbs_counts {cusing}'),
        ('MAPS counts', f'SELECT * FROM counts JOIN maps_counts {cusing}'),
        ('eBird counts', f'SELECT * FROM counts JOIN ebird_counts {cusing}'),
        ('*', 'You can join event and count records using the event_id.')]

    cxn = db.connect()

    report_name = f'query_samples_{datetime.now().strftime("%Y-%m-%d")}.xlsx'
    report_path = db.DATA_DIR / 'interim' / report_name

    with pd.ExcelWriter(report_path) as writer:
        index = pd.DataFrame(queries, columns=['Tab', 'Query'])
        index.to_excel(writer, sheet_name='Index', index=False)
        for label, sql in queries:
            if label.startswith('*'):
                continue
            df = pd.read_sql(sql, cxn)
            df.to_excel(writer, sheet_name=label, index=False)


if __name__ == '__main__':
    generate_samples()
