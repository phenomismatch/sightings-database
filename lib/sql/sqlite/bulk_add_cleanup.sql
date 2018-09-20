CREATE INDEX IF NOT EXISTS places_dataset_id ON places (dataset_id);
CREATE INDEX IF NOT EXISTS places_lng_lat    ON places (lng, lat);
CREATE INDEX IF NOT EXISTS places_geohash    ON places (geohash);
CREATE INDEX IF NOT EXISTS events_year_day   ON events (year, day);
CREATE INDEX IF NOT EXISTS counts_event_id   ON counts (event_id);
CREATE INDEX IF NOT EXISTS counts_taxon_id   ON counts (taxon_id);
VACUUM;
