-- CREATE EXTENSION "postgis";
-- CREATE EXTENSION "postgis_topology";


DROP TABLE IF EXISTS version;
DROP TABLE IF EXISTS datasets CASCADE;
DROP TABLE IF EXISTS taxons   CASCADE;
DROP TABLE IF EXISTS places   CASCADE;
DROP TABLE IF EXISTS events   CASCADE;
DROP TABLE IF EXISTS counts;


CREATE TABLE version (
  version VARCHAR(10) NOT NULL UNIQUE PRIMARY KEY,
  created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE datasets (
  dataset_id VARCHAR(10) NOT NULL UNIQUE PRIMARY KEY,
  title      VARCHAR(80) NOT NULL,
  version    VARCHAR(16) NOT NULL,
  url        VARCHAR(120),
  extracted  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE taxons (
  taxon_id    SERIAL PRIMARY KEY,
  dataset_id  TEXT REFERENCES datasets (dataset_id) ON DELETE CASCADE,
  sci_name    VARCHAR(80) NOT NULL UNIQUE,
  class       VARCHAR(20) NOT NULL,
  ordr        VARCHAR(40),
  family      VARCHAR(80),
  genus       VARCHAR(20),
  synonyms    VARCHAR(200),
  common_name VARCHAR(40),
  target      BOOLEAN
);
CREATE INDEX ON taxons (sci_name);
CREATE INDEX ON taxons (class);
CREATE INDEX ON taxons (ordr);
CREATE INDEX ON taxons (family);
CREATE INDEX ON taxons (genus);


CREATE TABLE places (
  place_id   SERIAL PRIMARY KEY,
  dataset_id TEXT REFERENCES datasets (dataset_id) ON DELETE CASCADE,
  lng        NUMERIC(4) NOT NULL,
  lat        NUMERIC(4) NOT NULL,
  radius     NUMERIC(4),
  geohash    VARCHAR(7),
  geopoint   GEOGRAPHY(POINT, 4326)
);
CREATE INDEX ON places (lng, lat);
CREATE INDEX ON places (geohash);


CREATE TABLE events (
  event_id SERIAL PRIMARY KEY,
  place_id INTEGER REFERENCES places (place_id) ON DELETE CASCADE,
  year     SMALLINT NOT NULL,
  day      SMALLINT NOT NULL,
  started  TIME,
  ended    TIME
);
CREATE INDEX ON events (year, day);


CREATE TABLE counts (
  count_id SERIAL PRIMARY KEY,
  event_id  INTEGER REFERENCES events  (event_id) ON DELETE CASCADE,
  taxon_id INTEGER REFERENCES taxons (taxon_id),
  count    INTEGER NOT NULL
);
