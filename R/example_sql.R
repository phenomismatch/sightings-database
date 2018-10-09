library(dplyr)
library(dbplyr)
library("RPostgreSQL")

# cxn <- src_sqlite(path = "data/processed/sightings.sqlite.db", create = FALSE)
pg <- dbDriver("PostgreSQL")
cxn <- dbConnect(pg, user = "username", password = "password",
                 host = "35.221.16.125", port = 5432, dbname = "sightings")

places <- tbl(cxn, "places")
events <- tbl(cxn, "events")
counts <- tbl(cxn, "counts")
taxons <- tbl(cxn, "taxons")


# "dplyr" is lazy and only evaluates as little as possible to create the desired output.
# This helps with efficiency. So, you may want to avoid the as.data.frame() function,
# it is there only to show how to convert the queries into a dataframe.

all_tables <- places %>%
  filter(between(lng, -73, -72)) %>%
  filter(between(lat, 40, 44)) %>%
  inner_join(events, by = "place_id") %>%
  filter(between(year, 2010, 2014)) %>%
  filter(between(day, 100, 200)) %>%
  inner_join(counts, by = "event_id") %>%
  inner_join(taxons, by = "taxon_id") %>%
  head(100) %>%
  as.data.frame()
