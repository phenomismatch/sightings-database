-- split --additional-suffix=.csv --numeric-suffixes=1 --lines=10000000 counts_ebird.csv counts_ebird_
-- split --additional-suffix=.csv --numeric-suffixes=1 --lines=5000000 events_ebird.csv events_ebird_

psql "sslmode=disable dbname=sightings user=<username> hostaddr=35.221.16.125"
psql "sslmode=disable dbname=sightings user=<username> hostaddr=localhost"

\copy datasets FROM '<source dir>/datasets.csv' WITH (FORMAT csv);

\copy taxa FROM '<source dir>/taxa.csv' WITH (FORMAT csv);

\copy places FROM '<source dir>/places.csv' WITH (FORMAT csv);

\copy events FROM '<source dir>/events.csv' WITH (FORMAT csv);

\copy counts FROM '<source dir>/counts.csv' WITH (FORMAT csv);

UPDATE places
   SET geopoint = ST_SetSRID(ST_MakePoint(lng, lat), 4326),
       geohash  = ST_GeoHash(ST_MakePoint(lng, lat), 7)
 WHERE dataset_id = ?;
COMMIT;
