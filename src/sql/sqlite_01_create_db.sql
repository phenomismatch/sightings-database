DROP TABLE IF EXISTS version;
CREATE TABLE version (
  version      TEXT NOT NULL PRIMARY KEY,
  version_date TEXT NOT NULL
);


DROP TABLE IF EXISTS datasets;
CREATE TABLE datasets (
  dataset_id TEXT NOT NULL PRIMARY KEY,
  title      TEXT NOT NULL,
  version    TEXT NOT NULL,
  extracted  TEXT NOT NULL,
  url        TEXT
);


DROP TABLE IF EXISTS taxons;
CREATE TABLE taxons (
  taxon_id  INTEGER NOT NULL PRIMARY KEY,
  sci_name  TEXT NOT NULL UNIQUE,
  class     TEXT NOT NULL,
  ordr      TEXT,
  family    TEXT,
  genus     TEXT,
  is_target INTEGER,
  common_name TEXT
);
CREATE INDEX taxons_scientific_name ON taxons (sci_name);
CREATE INDEX taxons_class  ON taxons (class);
CREATE INDEX taxons_order  ON taxons (ordr);
CREATE INDEX taxons_family ON taxons (family);
CREATE INDEX taxons_genus  ON taxons (genus);


DROP TABLE IF EXISTS events;
CREATE TABLE events (
  event_id    INTEGER NOT NULL PRIMARY KEY,
  dataset_id  TEXT NOT NULL,
  year        NUMBER NOT NULL,
  day         NUMBER NOT NULL,
  start_time  TEXT,
  end_time    TEXT,
  latitude    NUMBER NOT NULL,
  longitude   NUMBER NOT NULL,
  radius      NUMBER,
  geohash     TEXT
);
CREATE INDEX events_dataset_id   ON events (dataset_id);
CREATE INDEX events_date_lng_lat ON events (year, day, longitude, latitude);
CREATE INDEX events_date_geohash ON events (year, day, geohash);
SELECT AddGeometryColumn('events', 'point', 4326, 'POINT', 'XY', 0);
SELECT CreateSpatialIndex('events', 'point');


DROP TABLE IF EXISTS counts;
CREATE TABLE counts (
  count_id    INTEGER NOT NULL PRIMARY KEY,
  event_id    INTEGER NOT NULL,
  taxon_id    INTEGER NOT NULL,
  count       INTEGER NOT NULL
);
CREATE INDEX counts_event_id ON counts (event_id);
CREATE INDEX counts_taxon_id ON counts (taxon_id);
CREATE INDEX counts_event_taxon ON counts (event_id, taxon_id);
