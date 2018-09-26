DROP TABLE IF EXISTS version;
CREATE TABLE version (
  version VARCHAR(10) PRIMARY KEY,
  created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS datasets;
CREATE TABLE datasets (
  dataset_id VARCHAR(12) NOT NULL PRIMARY KEY,
  title      VARCHAR(80) NOT NULL,
  version    VARCHAR(16) NOT NULL,
  url        VARCHAR(120),
  extracted  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS countries;
CREATE TABLE countries (
  code       INTEGER NOT NULL PRIMARY KEY,
  short_name VARCHAR(80),
  alpha2     VARCHAR(2),
  alpha3     VARCHAR(3)
);
CREATE INDEX countries_alpha2 ON countries(alpha2);
CREATE INDEX countries_alpha3 ON countries(alpha3);


DROP TABLE IF EXISTS codes;
CREATE TABLE codes (
  dataset_id VARCHAR(12) NOT NULL,
  field      VARCHAR(50) NOT NULL,
  code       VARCHAR(50) NOT NULL,
  value      TEXT
);
CREATE INDEX codes_field ON codes (dataset_id, field);
CREATE INDEX codes_code  ON codes (dataset_id, code);


DROP TABLE IF EXISTS taxons;
CREATE TABLE taxons (
  taxon_id    INTEGER NOT NULL PRIMARY KEY,
  authority   VARCHAR(12) NOT NULL,
  sci_name    VARCHAR(80) NOT NULL UNIQUE,
  "group"     VARCHAR(40),
  "class"     VARCHAR(20),
  "order"     VARCHAR(40),
  family      VARCHAR(80),
  genus       VARCHAR(20),
  common_name VARCHAR(120),
  target      BOOLEAN
);
CREATE INDEX taxons_dataset_id ON taxons (authority);
CREATE INDEX taxons_sci_name   ON taxons (sci_name);
CREATE INDEX taxons_group  ON taxons ("group");
CREATE INDEX taxons_class  ON taxons ("class");
CREATE INDEX taxons_order  ON taxons ("order");
CREATE INDEX taxons_family ON taxons (family);
CREATE INDEX taxons_genus  ON taxons (genus);


DROP TABLE IF EXISTS places;
CREATE TABLE places (
  place_id   INTEGER NOT NULL PRIMARY KEY,
  dataset_id VARCHAR(12) NOT NULL,
  lng        NUMERIC NOT NULL,
  lat        NUMERIC NOT NULL,
  radius     NUMERIC,
  place_json TEXT
  geohash    VARCHAR(8),
  geopoint   TEXT,
);
CREATE INDEX places_dataset_id ON places (dataset_id);
CREATE INDEX places_lng_lat    ON places (lng, lat);
CREATE INDEX places_geohash    ON places (geohash);


DROP TABLE IF EXISTS events;
CREATE TABLE events (
  event_id   INTEGER NOT NULL PRIMARY KEY,
  place_id   INTEGER NOT NULL,
  year       INTEGER NOT NULL,
  day        INTEGER NOT NULL,
  started    TEXT,
  ended      TEXT,
  event_json TEXT
);
CREATE INDEX events_year_day ON events (year, day);


DROP TABLE IF EXISTS counts;
CREATE TABLE counts (
  count_id   INTEGER NOT NULL PRIMARY KEY,
  event_id   INTEGER NOT NULL,
  taxon_id   INTEGER NOT NULL,
  count      INTEGER NOT NULL,
  count_json TEXT
);
CREATE INDEX counts_event_id ON counts (event_id);
CREATE INDEX counts_taxon_id ON counts (taxon_id);
