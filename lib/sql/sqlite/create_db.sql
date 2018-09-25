DROP TABLE IF EXISTS version;
CREATE TABLE version (
  version TEXT NOT NULL PRIMARY KEY,
  created TEXT NOT NULL
);


DROP TABLE IF EXISTS datasets;
CREATE TABLE datasets (
  dataset_id TEXT NOT NULL PRIMARY KEY,
  title      TEXT NOT NULL,
  version    TEXT NOT NULL,
  extracted  TEXT NOT NULL,
  url        TEXT
);


DROP TABLE IF EXISTS countries;
CREATE TABLE countries (
  code       INTEGER NOT NULL PRIMARY KEY,
  short_name TEXT NOT NULL,
  alpha2     TEXT NOT NULL,
  alpha3     TEXT NOT NULL
);
CREATE INDEX countries_code ON countries(code);
CREATE INDEX countries_alpha2 ON countries(alpha2);
CREATE INDEX countries_alpha3 ON countries(alpha3);


DROP TABLE IF EXISTS codes;
CREATE TABLE codes (
  dataset_id TEXT NOT NULL,
  field      TEXT NOT NULL,
  code       TEXT NOT NULL,
  value      TEXT
);
CREATE INDEX codes_field ON codes (dataset_id, field);
CREATE INDEX codes_code  ON codes (dataset_id, code);


DROP TABLE IF EXISTS taxons;
CREATE TABLE taxons (
  taxon_id    INTEGER NOT NULL PRIMARY KEY,
  authority   TEXT NOT NULL,
  sci_name    TEXT UNIQUE,
  "group"     TEXT,
  "class"     TEXT,
  "order"     TEXT,
  family      TEXT,
  genus       TEXT,
  common_name TEXT,
  target      TEXT
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
  dataset_id TEXT NOT NULL,
  lng        NUMBER NOT NULL,
  lat        NUMBER NOT NULL,
  radius     NUMBER,
  geohash    TEXT,
  place_json TEXT
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