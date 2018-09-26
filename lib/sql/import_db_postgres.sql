\! date
COPY version FROM '/museum/rafe/sightings/data/interim/version.csv' WITH (FORMAT csv);

\! date
COPY datasets FROM '/museum/rafe/sightings/data/interim/datasets.csv' WITH (FORMAT csv);

\! date
COPY countries FROM '/museum/rafe/sightings/data/interim/countries.csv' WITH (FORMAT csv);

\! date
COPY codes FROM '/museum/rafe/sightings/data/interim/codes.csv' WITH (FORMAT csv);

\! date
COPY taxons FROM '/museum/rafe/sightings/data/interim/taxons.csv' WITH (FORMAT csv);

\! date
COPY places FROM '/museum/rafe/sightings/data/interim/places.csv' WITH (FORMAT csv);

\! date
COPY events FROM '/museum/rafe/sightings/data/interim/events.csv' WITH (FORMAT csv);

\! date
COPY counts FROM '/museum/rafe/sightings/data/interim/counts.csv' WITH (FORMAT csv);

\! date
UPDATE places
   SET geopoint = ST_SetSRID(ST_MakePoint(lng, lat), 4326),
       geohash  = ST_GeoHash(ST_MakePoint(lng, lat), 7);
COMMIT;

\! date
