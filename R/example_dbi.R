library(DBI)
library(RPostgreSQL)

cxn <- dbConnect(
  PostgreSQL(),
  user = "rlafrance",
  password = rstudioapi::askForPassword("Database password"),
  host = "35.221.16.125",
  port = 5432,
  dbname = "sightings")

result <- dbGetQuery(cxn,
"SELECT lng, lat, year, day, count, sci_name,
        event_json ->> 'GROUP_IDENTIFIER' AS group_identifier
   FROM places
   JOIN events USING (place_id)
   JOIN counts USING (event_id)
   JOIN taxons USING (taxon_id)
  WHERE dataset_id = 'ebird'
    AND year = 2014
    AND day  BETWEEN  100 AND  110
    AND lng  BETWEEN  -73 AND  -72
    AND lat  BETWEEN   40 AND   41
    AND target = 't'
    AND event_json -> 'GROUP_IDENTIFIER' IS NOT NULL;")

dbDisconnect(cxn)
