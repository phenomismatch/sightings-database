DROP TABLE IF EXISTS datasets;
CREATE TABLE datasets (
  dataset_id VARCHAR(12) NOT NULL PRIMARY KEY,
  title      VARCHAR(80) NOT NULL,
  version    VARCHAR(16) NOT NULL,
  url        VARCHAR(120),
  extracted  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS taxons;
CREATE TABLE taxons (
  taxon_id    INTEGER NOT NULL PRIMARY KEY,
  dataset_id  VARCHAR(12) NOT NULL,
  sci_name    VARCHAR(80) NOT NULL UNIQUE,
  "group"     VARCHAR(40),
  "class"     VARCHAR(20),
  "order"     VARCHAR(40),
  family      VARCHAR(80),
  genus       VARCHAR(20),
  common_name VARCHAR(120),
  target      BOOLEAN
);
CREATE INDEX taxons_dataset_id ON taxons (dataset_id);
CREATE INDEX taxons_sci_name   ON taxons (sci_name);
CREATE INDEX taxons_group  ON taxons ("group");
CREATE INDEX taxons_class  ON taxons ("class");
CREATE INDEX taxons_order  ON taxons ("order");
CREATE INDEX taxons_family ON taxons (family);
CREATE INDEX taxons_genus  ON taxons (genus);
CREATE INDEX taxons_target ON taxons (target);


DROP TABLE IF EXISTS places;
CREATE TABLE places (
  place_id   INTEGER NOT NULL PRIMARY KEY,
  dataset_id VARCHAR(12) NOT NULL,
  lng        NUMERIC NOT NULL,
  lat        NUMERIC NOT NULL,
  radius     NUMERIC,
  place_json TEXT,
  geohash    VARCHAR(8),
  geopoint   TEXT
);
CREATE INDEX places_dataset_id ON places (dataset_id);
CREATE INDEX places_lng        ON places (lng);
CREATE INDEX places_lat        ON places (lat);
CREATE INDEX places_geohash    ON places (geohash);


DROP TABLE IF EXISTS events;
CREATE TABLE events (
  event_id   INTEGER NOT NULL PRIMARY KEY,
  place_id   INTEGER NOT NULL,
  dataset_id VARCHAR(12) NOT NULL,
  year       INTEGER NOT NULL,
  day        INTEGER NOT NULL,
  started    TEXT,
  ended      TEXT,
  event_json TEXT
);
CREATE INDEX events_place_id   ON events (place_id);
CREATE INDEX events_dataset_id ON events (dataset_id);
CREATE INDEX events_year       ON events (year);
CREATE INDEX events_day        ON events (day);


DROP TABLE IF EXISTS counts;
CREATE TABLE counts (
  count_id   INTEGER NOT NULL PRIMARY KEY,
  event_id   INTEGER NOT NULL,
  dataset_id VARCHAR(12) NOT NULL,
  taxon_id   INTEGER NOT NULL,
  count      INTEGER NOT NULL,
  count_json TEXT
);
CREATE INDEX counts_event_id   ON counts (event_id);
CREATE INDEX counts_taxon_id   ON counts (taxon_id);
CREATE INDEX counts_dataset_id ON counts (dataset_id);
