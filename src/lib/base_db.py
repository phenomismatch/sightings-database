"""Common functions for dealing with database connections."""


class BaseDb:
    """Common database functions."""

    def delete_dataset(self, dataset_id):
        """Clear dataset from the database."""
        print(f'Deleting old {dataset_id} records')

        self.cxn.execute(
            "DELETE FROM datasets WHERE dataset_id = ?", (dataset_id, ))

        self.cxn.commit()

        for sidecar in ['codes', 'places', 'events', 'counts']:
            self.cxn.execute(f'DROP TABLE IF EXISTS {dataset_id}_{sidecar}')
