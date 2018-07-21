-- CREATE EXTENSION "postgis";
-- CREATE EXTENSION "postgis_topology";


DROP TABLE IF EXISTS version;
DROP TABLE IF EXISTS datasets CASCADE;
DROP TABLE IF EXISTS taxons   CASCADE;
DROP TABLE IF EXISTS points   CASCADE;
DROP TABLE IF EXISTS dates    CASCADE;
DROP TABLE IF EXISTS counts;


CREATE TABLE version (
  version VARCHAR(10) NOT NULL PRIMARY KEY,
  created TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE datasets (
  dataset_id VARCHAR(10) NOT NULL PRIMARY KEY,
  title      VARCHAR(80) NOT NULL,
  version    VARCHAR(16) NOT NULL,
  url        VARCHAR(120),
  extracted  DATE
);


CREATE TABLE taxons (
  taxon_id    SERIAL PRIMARY KEY,
  dataset_id  TEXT REFERENCES datasets (dataset_id),
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


CREATE TABLE points (
  point_id   SERIAL PRIMARY KEY,
  dataset_id TEXT REFERENCES datasets (dataset_id),
  lng        NUMERIC(4) NOT NULL,
  lat        NUMERIC(4) NOT NULL,
  radius     NUMERIC(4),
  geohash    VARCHAR(7),
  geopoint   GEOGRAPHY(POINT, 4326)
);
CREATE INDEX ON points (lng, lat);
CREATE INDEX ON points (geohash);


CREATE TABLE dates (
  date_id  SERIAL PRIMARY KEY,
  point_id INTEGER REFERENCES points (point_id),
  year     SMALLINT NOT NULL,
  day      SMALLINT NOT NULL,
  started  TIME,
  ended    TIME
);
CREATE INDEX ON dates (year, day);


CREATE TABLE counts (
  count_id SERIAL PRIMARY KEY,
  date_id  INTEGER REFERENCES dates  (date_id),
  taxon_id INTEGER REFERENCES taxons (taxon_id),
  count    INTEGER NOT NULL
);
