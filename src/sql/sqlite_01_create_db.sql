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


DROP TABLE IF EXISTS taxons;
CREATE TABLE taxons (
  taxon_id    INTEGER NOT NULL PRIMARY KEY,
  dataset_id  TEXT NOT NULL,
  sci_name    TEXT NOT NULL UNIQUE,
  class       TEXT NOT NULL,
  ordr        TEXT,
  family      TEXT,
  genus       TEXT,
  synonyms    TEXT,
  common_name TEXT,
  target      INTEGER
);
CREATE INDEX taxons_sci_name   ON taxons (sci_name);
CREATE INDEX taxons_dataset_id ON taxons (dataset_id);
CREATE INDEX taxons_class  ON taxons (class);
CREATE INDEX taxons_order  ON taxons (ordr);
CREATE INDEX taxons_family ON taxons (family);
CREATE INDEX taxons_genus  ON taxons (genus);


DROP TABLE IF EXISTS points;
CREATE TABLE points (
  point_id   INTEGER NOT NULL PRIMARY KEY,
  dataset_id TEXT NOT NULL,
  lng        NUMBER NOT NULL,
  lat        NUMBER NOT NULL,
  geohash    TEXT,
  radius     NUMBER
);
CREATE INDEX points_dataset_id ON points (dataset_id);
CREATE INDEX points_lng_lat    ON points (lng, lat);
CREATE INDEX points_geohash    ON points (geohash);
SELECT AddGeometryColumn('points', 'geopoint', 4326, 'POINT', 'XY', 0);
SELECT CreateSpatialIndex('points', 'geopoint');


DROP TABLE IF EXISTS dates;
CREATE TABLE dates (
  date_id  INTEGER NOT NULL PRIMARY KEY,
  point_id INTEGER NOT NULL,
  year     INTEGER NOT NULL,
  day      INTEGER NOT NULL,
  started  TEXT,
  ended    TEXT
);
CREATE INDEX dates_point_id ON dates (point_id);
CREATE INDEX dates_year_day ON dates (year, day);


DROP TABLE IF EXISTS counts;
CREATE TABLE counts (
  count_id INTEGER NOT NULL PRIMARY KEY,
  date_id  INTEGER NOT NULL,
  taxon_id INTEGER NOT NULL,
  count    INTEGER NOT NULL
);
CREATE INDEX counts_event_id ON counts (date_id);
CREATE INDEX counts_taxon_id ON counts (taxon_id);
