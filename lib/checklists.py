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

    db.insert_checklists(checklists)
    db.update_event_checklists(events)
    db.delete_ebird_0_counts()
    db.vacuum()


if __name__ == '__main__':
    build()
