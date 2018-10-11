library(dplyr)
library(dbplyr)
library(RPostgreSQL)

# cxn <- src_sqlite(path = "data/processed/sightings.sqlite.db", create = FALSE)
cxn <- dbConnect(
  dbDriver("PostgreSQL"),
  user = "username",
  # password = "password",
  password = rstudioapi::askForPassword("Database password"),
  host = "35.221.16.125",
  # host = "localhost",
  port = 5432,
  dbname = "sightings")

places <- tbl(cxn, "places")
events <- tbl(cxn, "events")
counts <- tbl(cxn, "counts")
taxons <- tbl(cxn, "taxons")


# "dplyr" is lazy and only evaluates as little as possible to create the desired output.
# This helps with efficiency. So, you may want to avoid the as.data.frame() function,
# it is there only to show how to convert the queries into a dataframe.

all_tables <- places %>%
  inner_join(events, by = "place_id") %>%
  inner_join(counts, by = "event_id") %>%
  inner_join(taxons, by = "taxon_id") %>%
  filter(
    dataset_id == 'ebird',
    year == 2014,
    between(lng, -73, -72),
    between(lat, 40, 44),
    between(day, 100, 200),
    target == "t")  %>%
  select(lng, lat, year, day, count, sci_name, event_json) %>%
  head(100) %>%
  as.data.frame()
  #show_query()

event_json <- lapply(all_tables$event_json, fromJSON)
