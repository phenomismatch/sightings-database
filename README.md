# pheno-mismatch
Causes, consequences, and cross-scale linkages of climate-driven phenological mismatch across three tropic levels

## Repository contribution
This repository contains scripts to help research and outreach efforts. There are other GitHub repositories for this project maintained by the various research labs.

## Database
Currently, all data is stored in a single SQLite3 or PostgreSQL (PostGIS) database. We have gathered and consolidated the data from these sources:

1. North American Breeding Bird Survey (BBS).
1. MAPS: Monitoring Avian Productivity and Survivorship.
1. eBird Basic Dataset. This dataset has been culled to take everything between 20⁰ and 90⁰ latitude and -95⁰ and -50⁰ longitude. We are also only taking "complete" and "approved" checklists. We are also only taking data for 117 migratory bird species.
1. Pollard butterfly dataset.
1. NABA butterfly dataset.
1. Caterpillar Counts dataset.
1. NestWatch dataset.

#### Samples in R
See this R [script](R/example_sql.R) for how to access the spatialite database in R.

#### Samples in Python
Most of the scripts in the src directory access the database. I have moved common code into this [library](src/lib/sqlite.py). A some sample_queries that use this library are in this Python [script](src/example_sql.py).

#### When building SQLite3
```
CFLAGS="-O2 -DSQLITE_ENABLE_COLUMN_METADATA -DSQLITE_ENABLE_FTS3 -DSQLITE_ENABLE_FTS4 -DSQLITE_ENABLE_FTS5 -DSQLITE_ENABLE_GEOPOLY -DSQLITE_ENABLE_JSON1 -DSQLITE_ENABLE_RTREE -DHAVE_READLINE" ./configure --enable-readline=yes
```

![Output image](docs/schema/schema_1.png "Database Schema")
