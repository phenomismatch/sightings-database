"""Create checklists."""

import lib.db as db
from lib.util import log


def build():
    """Create eBird checklists."""
    log('Building eBird checklists')

    events = {}
    checklists = {}
    checklist_id = 0

    for event_id, taxon_ids in db.taxon_ids_per_event():
        taxon_ids = ' '.join(sorted(taxon_ids.split()))

        if not checklists.get(taxon_ids):
            checklist_id += 1
            checklists[taxon_ids] = checklist_id

        events[event_id] = checklists[taxon_ids]

        print(event_id, taxon_ids)

        # Create checklists
        #   insert into checklists (checklist_id, taxon_id) values (?, ?)
        # Update events with checklist_ids
        #   cxn.executemany(
        #       'update events set checklist_id=? where event_id=?', lists)
        # delete from counts where dataset_id = 'ebird' and count = 0
        # vacuum

if __name__ == '__main__':
    build()
