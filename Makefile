SHELL=/bin/bash
DATE=`date +%Y-%m-%d`
PYTHON=python
PROCESSED=./data/processed
RAW=./data/raw

SQLITE_DB=sightings.sqlite.db
SQLITE_SRC="$(PROCESSED)/$(SQLITE_DB)"
SQLITE_DST="$(PROCESSED)/$(basename $(SQLITE_DB))_$(DATE).db"

all_postgres: create_postgres bbs_postgres maps_postgres ebird_postgres pollard_postgres naba_postgres

create_postgres:
	$(PYTHON) ./src/postgres_create_db.py

bbs_postgres:
	$(PYTHON) ./src/postgres_ingest_bbs.py

maps_postgres:
	$(PYTHON) ./src/postgres_ingest_maps.py

ebird_postgres:
	$(PYTHON) ./src/postgres_ingest_ebird.py

pollard_postgres:
	$(PYTHON) ./src/postgres_ingest_pollard.py

naba_postgres:
	$(PYTHON) ./src/postgres_ingest_naba.py

all_sqlite: clean_sqlite create_sqlite bbs_sqlite maps_sqlite ebird_sqlite pollard_sqlite naba_sqlite

create_sqlite:
	$(PYTHON) ./src/sqlite_create_db.py

bbs_sqlite:
	$(PYTHON) ./src/sqlite_ingest_bbs.py

maps_sqlite:
	$(PYTHON) ./src/sqlite_ingest_maps.py

ebird_sqlite:
	$(PYTHON) ./src/sqlite_ingest_ebird.py

pollard_sqlite:
	$(PYTHON) ./src/sqlite_ingest_pollard.py

naba_sqlite:
	$(PYTHON) ./src/sqlite_ingest_naba.py

backup_sqlite:
	cp $(SQLITE_SRC) $(SQLITE_DST)

clean_sqlite:
	rm -f $(SQLITE_SRC)
