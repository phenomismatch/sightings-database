SHELL=/bin/bash
DATE=`date +%Y-%m-%d`
PYTHON=python
PROCESSED=./data/processed
RAW=./data/raw

SQLITE_DB=sightings.sqlite.db
SQLITE_SRC="$(PROCESSED)/$(SQLITE_DB)"
SQLITE_DST="$(PROCESSED)/$(basename $(SQLITE_DB))_$(DATE).db"

all_postgres: create_postgres bbs_postgres maps_postgres ebird_postgres pollard_postgres

create_postgres:
	$(PYTHON) ./src/postgres_01_create_db.py

bbs_postgres:
	$(PYTHON) ./src/postgres_02_ingest_bbs.py

maps_postgres:
	$(PYTHON) ./src/postgres_03_ingest_maps.py

ebird_postgres:
	$(PYTHON) ./src/postgres_04_ingest_ebird.py

pollard_postgres:
	$(PYTHON) ./src/postgres_05_ingest_pollard.py

all_sqlite: clean_sqlite create_sqlite bbs_sqlite maps_sqlite ebird_sqlite pollard_sqlite

create_sqlite:
	$(PYTHON) ./src/sqlite_01_create_db.py

bbs_sqlite:
	$(PYTHON) ./src/sqlite_02_ingest_bbs.py

maps_sqlite:
	$(PYTHON) ./src/sqlite_03_ingest_maps.py

ebird_sqlite:
	$(PYTHON) ./src/sqlite_04_ingest_ebird.py

pollard_sqlite:
	$(PYTHON) ./src/sqlite_05_ingest_pollard.py

backup_sqlite:
	cp $(SQLITE_SRC) $(SQLITE_DST)

clean_sqlite:
	rm -f $(SQLITE_SRC)
