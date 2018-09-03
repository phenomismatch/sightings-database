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
	$(PYTHON) ./src/create_db_postgres.py

bbs_postgres:
	$(PYTHON) ./src/bbs_ingest_postgres.py

maps_postgres:
	$(PYTHON) ./src/maps_ingest_postgres.py

ebird_postgres:
	$(PYTHON) ./src/ebird_ingest_postgres.py

pollard_postgres:
	$(PYTHON) ./src/pollard_ingest_postgresrd.py

naba_postgres:
	$(PYTHON) ./src/naba_ingest_postgrespy

all_sqlite: clean_sqlite create_sqlite bbs_sqlite maps_sqlite ebird_sqlite pollard_sqlite naba_sqlite

create_sqlite:
	$(PYTHON) ./src/create_db_sqlite.py

bbs_sqlite:
	$(PYTHON) ./src/bbs_ingest_sqlite.py

maps_sqlite:
	$(PYTHON) ./src/maps_ingest_sqlite.py

ebird_sqlite:
	$(PYTHON) ./src/ebird_ingest_sqlited.py

pollard_sqlite:
	$(PYTHON) ./src/pollard_ingest_sqliteard.py

naba_sqlite:
	$(PYTHON) ./src/naba_ingest_sqlite.py

backup_sqlite:
	cp $(SQLITE_SRC) $(SQLITE_DST)

clean_sqlite:
	rm -f $(SQLITE_SRC)
