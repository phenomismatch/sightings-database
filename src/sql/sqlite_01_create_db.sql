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
  target      TEXT
  -- FOREIGN KEY (dataset_id) REFERENCES datasets (dataset_id) ON DELETE CASCADE
);
CREATE INDEX taxons_sci_name ON taxons (sci_name);
CREATE INDEX taxons_class  ON taxons (class);
CREATE INDEX taxons_order  ON taxons (ordr);
CREATE INDEX taxons_family ON taxons (family);
CREATE INDEX taxons_genus  ON taxons (genus);


DROP TABLE IF EXISTS places;
CREATE TABLE places (
  place_id   INTEGER NOT NULL PRIMARY KEY,
  dataset_id TEXT NOT NULL,
  lng        NUMBER NOT NULL,
  lat        NUMBER NOT NULL,
  geohash    TEXT,
  radius     NUMBER
  -- FOREIGN KEY (dataset_id) REFERENCES datasets (dataset_id) ON DELETE CASCADE
);
CREATE INDEX places_lng_lat ON places (lng, lat);
CREATE INDEX places_geohash ON places (geohash);
SELECT AddGeometryColumn('places', 'geopoint', 4326, 'POINT', 'XY', 0);
SELECT CreateSpatialIndex('places', 'geopoint');


DROP TABLE IF EXISTS events;
CREATE TABLE events (
  event_id INTEGER NOT NULL PRIMARY KEY,
  place_id INTEGER NOT NULL,
  year     INTEGER NOT NULL,
  day      INTEGER NOT NULL,
  started  TEXT,
  ended    TEXT
  -- FOREIGN KEY (place_id) REFERENCES places (place_id) ON DELETE CASCADE
);
CREATE INDEX events_year_day ON events (year, day);


DROP TABLE IF EXISTS counts;
CREATE TABLE counts (
  count_id INTEGER NOT NULL PRIMARY KEY,
  event_id  INTEGER NOT NULL,
  taxon_id INTEGER NOT NULL,
  count    INTEGER NOT NULL
  -- FOREIGN KEY (event_id) REFERENCES events  (event_id) ON DELETE CASCADE,
  -- FOREIGN KEY (taxon_id) REFERENCES taxons (taxon_id) ON DELETE CASCADE
);
CREATE INDEX counts_event_id ON counts (event_id);
CREATE INDEX counts_taxon_id ON counts (taxon_id);
