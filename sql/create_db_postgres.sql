-- CREATE EXTENSION IF NOT EXISTS "postgis";
-- CREATE EXTENSION IF NOT EXISTS "postgis_topology";

-- CREATE ROLE sightings_user NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO sightings_user;
-- GRANT sightings_user TO username;

DROP TABLE IF EXISTS datasets CASCADE;
DROP TABLE IF EXISTS taxa     CASCADE;
DROP TABLE IF EXISTS places   CASCADE;
DROP TABLE IF EXISTS events   CASCADE;
DROP TABLE IF EXISTS counts;


CREATE TABLE datasets (
  dataset_id VARCHAR(12) PRIMARY KEY,
  title      VARCHAR(80) NOT NULL,
  version    VARCHAR(16) NOT NULL,
  url        VARCHAR(120),
  extracted  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE taxa (
  taxon_id    INTEGER PRIMARY KEY,
  sci_name    VARCHAR(80) UNIQUE,
  "group"     VARCHAR(80),
  "class"     VARCHAR(80),
  "order"     VARCHAR(80),
  family      VARCHAR(80),
  genus       VARCHAR(80),
  common_name VARCHAR(120),
  category    VARCHAR(80),
  target      BOOLEAN,
  revised_id  INTEGER,
  taxon_json  JSON
);
CREATE INDEX taxa_sci_name ON taxa (sci_name);
CREATE INDEX taxa_class  ON taxa ("class");
CREATE INDEX taxa_order  ON taxa ("order");
CREATE INDEX taxa_group  ON taxa ("group");
CREATE INDEX taxa_family ON taxa (family);
CREATE INDEX taxa_genus  ON taxa (genus);
CREATE INDEX taxa_target ON taxa (target);
CREATE INDEX taxa_category   ON taxa (category);
CREATE INDEX taxa_revised_id ON taxa (revised_id);


CREATE TABLE places (
  place_id   INTEGER PRIMARY KEY,
  dataset_id VARCHAR(12) REFERENCES datasets (dataset_id),
  lng        NUMERIC NOT NULL,
  lat        NUMERIC NOT NULL,
  radius     NUMERIC,
  place_json JSON,
  geohash    VARCHAR(8),
  geopoint   GEOGRAPHY(POINT, 4326)
);
CREATE INDEX places_dataset_id ON places (dataset_id);
CREATE INDEX places_lng        ON places (lng);
CREATE INDEX places_lat        ON places (lat);
CREATE INDEX places_geohash    ON places (geohash);


CREATE TABLE events (
  event_id   INTEGER PRIMARY KEY,
  place_id   INTEGER     REFERENCES places (place_id),
  dataset_id VARCHAR(12) REFERENCES datasets (dataset_id),
  year       INTEGER NOT NULL,
  day        INTEGER NOT NULL,
  started    VARCHAR(5),
  ended      VARCHAR(5),
  event_json JSON
);
CREATE INDEX events_place_id   ON events (place_id);
CREATE INDEX events_dataset_id ON events (dataset_id);
CREATE INDEX events_year       ON events (year);
CREATE INDEX events_day        ON events (day);


CREATE TABLE counts (
  count_id   INTEGER PRIMARY KEY,
  event_id   INTEGER     REFERENCES events (event_id),
  dataset_id VARCHAR(12) REFERENCES datasets (dataset_id),
  taxon_id   INTEGER     REFERENCES taxa (taxon_id),
  count      INTEGER NOT NULL,
  count_json JSON
);
CREATE INDEX counts_event_id   ON counts (event_id);
CREATE INDEX counts_taxon_id   ON counts (taxon_id);
CREATE INDEX counts_dataset_id ON counts (dataset_id);
