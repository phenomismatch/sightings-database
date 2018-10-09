CREATE EXTENSION IF NOT EXISTS "postgis";
CREATE EXTENSION IF NOT EXISTS "postgis_topology";


DROP TABLE IF EXISTS version;
DROP TABLE IF EXISTS countries;
DROP TABLE IF EXISTS datasets CASCADE;
DROP TABLE IF EXISTS taxons   CASCADE;
DROP TABLE IF EXISTS places   CASCADE;
DROP TABLE IF EXISTS events   CASCADE;
DROP TABLE IF EXISTS counts;
DROP TABLE IF EXISTS codes;


CREATE TABLE version (
  version VARCHAR(10) NOT NULL UNIQUE PRIMARY KEY,
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE datasets (
  dataset_id VARCHAR(12) PRIMARY KEY,
  title      VARCHAR(80) NOT NULL,
  version    VARCHAR(16) NOT NULL,
  url        VARCHAR(120),
  extracted  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE countries (
  code       INTEGER PRIMARY KEY,
  short_name VARCHAR(80),
  alpha2     VARCHAR(2),
  alpha3     VARCHAR(3)
);
CREATE INDEX countries_code ON countries(code);
CREATE INDEX countries_alpha2 ON countries(alpha2);
CREATE INDEX countries_alpha3 ON countries(alpha3);


CREATE TABLE codes (
  dataset_id VARCHAR(12) NOT NULL,
  field      VARCHAR(50) NOT NULL,
  code       VARCHAR(50) NOT NULL,
  value      TEXT
);
CREATE INDEX codes_field ON codes (dataset_id, field);
CREATE INDEX codes_code  ON codes (dataset_id, code);


CREATE TABLE taxons (
  taxon_id    INTEGER PRIMARY KEY,
  authority   VARCHAR(12) NOT NULL,
  sci_name    VARCHAR(80) UNIQUE,
  "group"     VARCHAR(40),
  "class"     VARCHAR(20),
  "order"     VARCHAR(40),
  family      VARCHAR(80),
  genus       VARCHAR(20),
  common_name VARCHAR(120),
  target      BOOLEAN
);
CREATE INDEX taxons_sci_name ON taxons (sci_name);
CREATE INDEX taxons_class    ON taxons ("class");
CREATE INDEX taxons_order    ON taxons ("order");
CREATE INDEX taxons_group    ON taxons ("group");
CREATE INDEX taxons_family   ON taxons (family);
CREATE INDEX taxons_genus    ON taxons (genus);
CREATE INDEX taxons_target   ON taxons (target);


CREATE TABLE places (
  place_id   INTEGER PRIMARY KEY,
  dataset_id VARCHAR(12) NOT NULL,
  lng        NUMERIC NOT NULL,
  lat        NUMERIC NOT NULL,
  radius     NUMERIC,
  place_json JSON,
  geohash    VARCHAR(8),
  geopoint   GEOGRAPHY(POINT, 4326),
  CONSTRAINT places_dataset_id FOREIGN KEY (dataset_id) REFERENCES datasets (dataset_id)
);
CREATE INDEX places_dataset_id ON places (dataset_id);
CREATE INDEX places_lng        ON places (lng);
CREATE INDEX places_lat        ON places (lat);
CREATE INDEX places_geohash    ON places (geohash);


CREATE TABLE events (
  event_id   INTEGER PRIMARY KEY,
  place_id   INTEGER NOT NULL,
  year       INTEGER NOT NULL,
  day        INTEGER NOT NULL,
  started    VARCHAR(5),
  ended      VARCHAR(5),
  event_json JSON,
  CONSTRAINT events_place_id FOREIGN KEY (place_id) REFERENCES places (place_id)
);
CREATE INDEX events_place_id ON events (place_id);
CREATE INDEX events_year     ON events (year);
CREATE INDEX events_day      ON events (day);


CREATE TABLE counts (
  count_id   INTEGER PRIMARY KEY,
  event_id   INTEGER NOT NULL,
  taxon_id   INTEGER NOT NULL,
  count      INTEGER NOT NULL,
  count_json JSON,
  CONSTRAINT counts_event_id FOREIGN KEY (event_id) REFERENCES events (event_id),
  CONSTRAINT counts_taxon_id FOREIGN KEY (taxon_id) REFERENCES taxons (taxon_id)
);
CREATE INDEX counts_event_id ON counts (event_id);
CREATE INDEX counts_taxon_id ON counts (taxon_id);
