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
  taxon_id    INTEGER PRIMARY KEY,
  dataset_id  TEXT REFERENCES datasets (dataset_id),
  sci_name    VARCHAR(80) NOT NULL UNIQUE,
  class       VARCHAR(20) NOT NULL,
  ordr        VARCHAR(40),
  family      VARCHAR(80),
  genus       VARCHAR(20),
  common_name VARCHAR(40),
  target      BOOLEAN
);
CREATE INDEX taxons_sci_name ON taxons (sci_name);
CREATE INDEX taxons_class    ON taxons (class);
CREATE INDEX taxons_ordr     ON taxons (ordr);
CREATE INDEX taxons_family   ON taxons (family);
CREATE INDEX taxons_genus    ON taxons (genus);


CREATE TABLE places (
  place_id   INTEGER NOT NULL,
  dataset_id TEXT    NOT NULL,
  lng        NUMERIC NOT NULL,
  lat        NUMERIC NOT NULL,
  radius     NUMERIC,
  geohash    VARCHAR(8),
  geopoint   GEOGRAPHY(POINT, 4326),
  CONSTRAINT places_place_id PRIMARY KEY (place_id),
  CONSTRAINT places_dataset_id FOREIGN KEY (dataset_id) REFERENCES datasets (dataset_id)
);
CREATE INDEX places_lng_lat ON places (lng, lat);
CREATE INDEX places_geohash ON places (geohash);


CREATE TABLE events (
  event_id INTEGER NOT NULL,
  place_id INTEGER NOT NULL,
  year     SMALLINT NOT NULL,
  day      SMALLINT NOT NULL,
  started  TIME,
  ended    TIME,
  CONSTRAINT events_event_id PRIMARY KEY (event_id),
  CONSTRAINT events_place_id FOREIGN KEY (place_id) REFERENCES places (place_id)
);
CREATE INDEX events_year_day ON events (year, day);


CREATE TABLE counts (
  count_id INTEGER NOT NULL,
  event_id INTEGER NOT NULL,
  taxon_id INTEGER NOT NULL,
  count    INTEGER NOT NULL,
  CONSTRAINT counts_count_id PRIMARY KEY (count_id),
  CONSTRAINT counts_event_id FOREIGN KEY (event_id) REFERENCES events (event_id),
  CONSTRAINT counts_taxon_id FOREIGN KEY (taxon_id) REFERENCES taxons (taxon_id)
);
