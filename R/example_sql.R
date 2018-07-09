library(dplyr)
library(dbplyr)

#setwd("work/sightings-database")
print(getwd())

cxn <- src_sqlite(path = "data/processed/sightings.sqlite.db", create = FALSE)

events <- tbl(cxn, "events")
counts <- tbl(cxn, "counts")
taxons <- tbl(cxn, "taxons")

bbs_events <- tbl(cxn, "bbs_events")
bbs_counts <- tbl(cxn, "bbs_counts")
maps_events <- tbl(cxn, "maps_events")
maps_counts <- tbl(cxn, "maps_counts")
ebird_events <- tbl(cxn, "ebird_events")
ebird_counts <- tbl(cxn, "ebird_counts")


# May want to avoid the as.data.frame() function, it is there only to show how to convert it.

combined = events %>%
  inner_join(counts, by = "event_id") %>%
  inner_join(taxons, by = "taxon_id") %>%
  filter(between(year, 2010, 2014)) %>%
  filter(between(day, 100, 120)) %>%
  filter(between(longitude, -79, -78)) %>%
  filter(between(latitude, 40, 41)) %>%
  head(100) %>%
  as.data.frame()


bbs = events %>%
  inner_join(counts, by = "event_id") %>%
  inner_join(taxons, by = "taxon_id") %>%
  inner_join(bbs_events, by = "event_id") %>%
  inner_join(bbs_counts, by = "count_id") %>%
  filter(between(year, 2010, 2014)) %>%
  filter(between(day, 100, 120)) %>%
  filter(between(longitude, -79, -78)) %>%
  filter(between(latitude, 40, 41)) %>%
  head(100) %>%
  as.data.frame()

maps = events %>%
  inner_join(counts, by = "event_id") %>%
  inner_join(taxons, by = "taxon_id") %>%
  inner_join(maps_events, by = "event_id") %>%
  inner_join(maps_counts, by = "count_id") %>%
  filter(between(year, 2010, 2014)) %>%
  filter(between(day, 100, 120)) %>%
  filter(between(longitude, -79, -78)) %>%
  filter(between(latitude, 40, 41)) %>%
  head(100) %>%
  as.data.frame()

ebird = events %>%
  inner_join(counts, by = "event_id") %>%
  inner_join(taxons, by = "taxon_id") %>%
  inner_join(ebird_events, by = "event_id") %>%
  inner_join(ebird_counts, by = "count_id") %>%
  filter(between(year, 2010, 2014)) %>%
  filter(between(day, 100, 120)) %>%
  filter(between(longitude, -79, -78)) %>%
  filter(between(latitude, 40, 41)) %>%
  head(100) %>%
  as.data.frame()
