\copy version FROM '/museum/rafe/sightings/data/interim/version.csv' WITH (FORMAT csv);

\copy datasets FROM '/museum/rafe/sightings/data/interim/datasets.csv' WITH (FORMAT csv);

\copy countries FROM '/museum/rafe/sightings/data/interim/countries.csv' WITH (FORMAT csv);

\copy codes FROM '/museum/rafe/sightings/data/interim/codes.csv' WITH (FORMAT csv);

\copy taxa FROM '/museum/rafe/sightings/data/interim/taxa.csv' WITH (FORMAT csv);

\copy places FROM '/museum/rafe/sightings/data/interim/places.csv' WITH (FORMAT csv);

\copy events FROM '/museum/rafe/sightings/data/interim/events.csv' WITH (FORMAT csv);

\copy counts FROM '/museum/rafe/sightings/data/interim/counts.csv' WITH (FORMAT csv);

UPDATE places
   SET geopoint = ST_SetSRID(ST_MakePoint(lng, lat), 4326),
       geohash  = ST_GeoHash(ST_MakePoint(lng, lat), 7);
COMMIT;
