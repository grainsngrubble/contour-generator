all: build/openmaptiles.tm2source/data.yml build/mapping.yaml build/tileset.sql

help:
	@echo "============================================================"
	@echo " ContourGenerator"
	@echo ""
	@echo "Core pipeline:"
	@echo "  make prefetch-contours"
	@echo "  make CONTOUR_JOBS=16 generate-contours-parallel"
	@echo "  make forced-clean-sql"
	@echo "  make psql-analyze"
	@echo "  make STREAM_POSTSERVE_REPLICAS=16 start-postserve-pool-safe"
	@echo "  make STREAM_MIN_ZOOM=10 STREAM_MAX_ZOOM=14 STREAM_PARALLEL_WORKERS=16 generate-mbtiles-parallel"
	@echo "  make verify-mbtiles-parallel"
	@echo ""
	@echo "Useful maintenance:"
	@echo "  make clean"
	@echo "  make clean-docker"
	@echo "  make psql"
	@echo "  make import-osm-dev"
	@echo "  make import-sql-dev"
	@echo "============================================================"

build/openmaptiles.tm2source/data.yml:
	mkdir -p build/openmaptiles.tm2source && generate-tm2source openmaptiles.yaml --host="postgres" --port=5432 --database="openmaptiles" --user="openmaptiles" --password="openmaptiles" > build/openmaptiles.tm2source/data.yml

build/mapping.yaml:
	mkdir -p build && generate-imposm3 openmaptiles.yaml > build/mapping.yaml

build/tileset.sql:
	mkdir -p build && generate-sql openmaptiles.yaml > build/tileset.sql

clean:
	rm -f build/openmaptiles.tm2source/data.yml && rm -f build/mapping.yaml && rm -f build/tileset.sql

PYTHON ?= python3
CONTOUR_HGTDIR ?= ./data/hgt
CONTOUR_JOBS ?= 8
CONTOUR_SOURCE ?= view1,view3,srtm3
CONTOUR_FLAGS ?=
STREAM_BBOX ?= -45,22,56,82
STREAM_MBTILES ?= ./data/tiles.mbtiles
STREAM_TILES_URL ?= http://$(shell postserve_id=$$(docker-compose ps -q postserve 2>/dev/null | head -n 1); if [ -n "$$postserve_id" ]; then docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $$postserve_id 2>/dev/null; else printf 'localhost'; fi):8080/tiles/{z}/{x}/{y}.pbf
STREAM_COMMIT_INTERVAL ?= 1
STREAM_REQUEST_TIMEOUT ?= 30
STREAM_REQUEST_TIMEOUT_MAX ?= 120
STREAM_TIMEOUT_RETRIES ?= 2
STREAM_TIMEOUT_RETRY_FACTOR ?= 2.0
STREAM_POSTGRES_TIMEOUT_BUFFER_MS ?= 1000
STREAM_DASHBOARD_REFRESH_MS ?= 250
STREAM_PARALLEL_WORKERS ?= 4
STREAM_POSTSERVE_REPLICAS ?= 4
STREAM_TARGET_TILES_PER_SHARD ?= 20000
STREAM_SHARD_DIR ?= ./data/tiles.parts
STREAM_FINAL_MBTILES ?= ./data/tiles.mbtiles
STREAM_SEED_MBTILES ?= ./data/tiles.mbtiles
STREAM_FLAGS ?= --resume --skip-existing

define require_stream_zoom_range
	$(if $(strip $(STREAM_MIN_ZOOM)),,$(error STREAM_MIN_ZOOM is required; pass STREAM_MIN_ZOOM=<min zoom>))
	$(if $(strip $(STREAM_MAX_ZOOM)),,$(error STREAM_MAX_ZOOM is required; pass STREAM_MAX_ZOOM=<max zoom>))
endef

clean-docker:
	docker-compose down -v --remove-orphans
	docker-compose rm -fv
	docker volume ls -q | grep openmaptiles  | xargs -r docker volume rm || true

list-docker-images:
	docker images | grep openmaptiles

refresh-docker-images:
	docker-compose pull --ignore-pull-failures

remove-docker-images:
	@echo "Deleting all openmaptiles related docker image(s)..."
	@docker-compose down
	@docker images | grep "openmaptiles" | awk -F" " '{print $$3}' | xargs --no-run-if-empty docker rmi -f
	@docker images | grep "klokantech/tileserver-gl"      | awk -F" " '{print $$3}' | xargs --no-run-if-empty docker rmi -f

docker-unnecessary-clean:
	@echo "Deleting unnecessary container(s)..."
	@docker ps -a  | grep Exited | awk -F" " '{print $$1}' | xargs  --no-run-if-empty docker rm
	@echo "Deleting unnecessary image(s)..."
	@docker images | grep \<none\> | awk -F" " '{print $$3}' | xargs  --no-run-if-empty  docker rmi

psql:
	docker-compose run --rm import-osm /usr/src/app/psql.sh

psql-list-tables:
	docker-compose run --rm import-osm /usr/src/app/psql.sh  -P pager=off  -c "\d+"

psql-pg-stat-reset:
	docker-compose run --rm import-osm /usr/src/app/psql.sh  -P pager=off  -c 'SELECT pg_stat_statements_reset();'

forced-clean-sql:
	docker-compose run --rm import-osm /usr/src/app/psql.sh -c "DROP SCHEMA IF EXISTS public CASCADE ; CREATE SCHEMA IF NOT EXISTS public; "
	docker-compose run --rm import-osm /usr/src/app/psql.sh -c "CREATE EXTENSION hstore; CREATE EXTENSION postgis; CREATE EXTENSION unaccent; CREATE EXTENSION fuzzystrmatch; CREATE EXTENSION osml10n; CREATE EXTENSION pg_stat_statements;"
	docker-compose run --rm import-osm /usr/src/app/psql.sh -c "GRANT ALL ON SCHEMA public TO public;COMMENT ON SCHEMA public IS 'standard public schema';"

pgclimb-list-views:
	docker-compose run --rm import-osm /usr/src/app/pgclimb.sh -c "select schemaname,viewname from pg_views where schemaname='public' order by viewname;" csv

pgclimb-list-tables:
	docker-compose run --rm import-osm /usr/src/app/pgclimb.sh -c "select schemaname,tablename from pg_tables where schemaname='public' order by tablename;" csv

psql-vacuum-analyze:
	@echo "Start - postgresql: VACUUM ANALYZE VERBOSE;"
	docker-compose run --rm import-osm /usr/src/app/psql.sh  -P pager=off  -c 'VACUUM ANALYZE VERBOSE;'

psql-analyze:
	@echo "Start - postgresql: ANALYZE VERBOSE ;"
	docker-compose run --rm import-osm /usr/src/app/psql.sh  -P pager=off  -c 'ANALYZE VERBOSE;'

start-postserve-safe:
	@echo "Starting postgres..."
	docker-compose up -d postgres
	@echo "Waiting for postgres readiness..."
	@i=0; \
	until docker-compose exec -T postgres pg_isready -U openmaptiles -d openmaptiles >/dev/null 2>&1; do \
		i=$$((i + 1)); \
		if [ $$i -ge 60 ]; then \
			echo "Postgres did not become ready in time"; \
			exit 1; \
		fi; \
		sleep 2; \
	done
	@echo "Starting postserve..."
	docker-compose up -d --build postserve
	@echo "Waiting for postserve HTTP readiness..."
	@i=0; \
	until postserve_id=$$(docker-compose ps -q postserve 2>/dev/null | head -n 1); \
		postserve_ip=$$(if [ -n "$$postserve_id" ]; then docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $$postserve_id 2>/dev/null; fi); \
		[ -n "$$postserve_ip" ] && curl -I --max-time 2 "http://$$postserve_ip:8080/health" >/dev/null 2>&1; do \
		i=$$((i + 1)); \
		if [ $$i -ge 60 ]; then \
			echo "postserve did not become ready in time"; \
			exit 1; \
		fi; \
		sleep 2; \
	done

start-postserve-pool-safe: start-postserve-safe
	@echo "Starting postserve worker pool..."
	docker-compose up -d --build --scale postserve-worker=$(STREAM_POSTSERVE_REPLICAS) postserve-worker
	@echo "Waiting for postserve worker HTTP readiness..."
	@i=0; \
	while true; do \
		ids=$$(docker-compose ps -q postserve-worker); \
		count=$$(printf '%s\n' "$$ids" | sed '/^$$/d' | wc -l); \
		if [ "$$count" -ge "$(STREAM_POSTSERVE_REPLICAS)" ]; then \
			ok=1; \
			for id in $$ids; do \
				ip=$$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $$id 2>/dev/null); \
				if [ -z "$$ip" ] || ! curl -I --max-time 2 "http://$$ip:8080/health" >/dev/null 2>&1; then \
					ok=0; \
					break; \
				fi; \
			done; \
			if [ "$$ok" -eq 1 ]; then \
				break; \
			fi; \
		fi; \
		i=$$((i + 1)); \
		if [ $$i -ge 60 ]; then \
			echo "postserve worker pool did not become ready in time"; \
			exit 1; \
		fi; \
		sleep 2; \
	done

generate-mbtiles-streaming:
	$(call require_stream_zoom_range)
	$(PYTHON) ./mbtiles/worker.py \
		--bbox="$(STREAM_BBOX)" \
		--min-zoom="$(STREAM_MIN_ZOOM)" \
		--max-zoom="$(STREAM_MAX_ZOOM)" \
		--mbtiles="$(STREAM_MBTILES)" \
		--tiles-url="$(STREAM_TILES_URL)" \
		--commit-interval="$(STREAM_COMMIT_INTERVAL)" \
		--request-timeout="$(STREAM_REQUEST_TIMEOUT)" \
		--max-request-timeout="$(STREAM_REQUEST_TIMEOUT_MAX)" \
		--timeout-retries="$(STREAM_TIMEOUT_RETRIES)" \
		--timeout-retry-factor="$(STREAM_TIMEOUT_RETRY_FACTOR)" \
		--postgres-timeout-buffer-ms="$(STREAM_POSTGRES_TIMEOUT_BUFFER_MS)" \
		--dashboard-refresh-ms="$(STREAM_DASHBOARD_REFRESH_MS)" \
		$(STREAM_FLAGS)

generate-mbtiles-parallel:
	$(call require_stream_zoom_range)
	$(PYTHON) ./mbtiles/export.py \
		--bbox="$(STREAM_BBOX)" \
		--min-zoom="$(STREAM_MIN_ZOOM)" \
		--max-zoom="$(STREAM_MAX_ZOOM)" \
		--final-mbtiles="$(STREAM_FINAL_MBTILES)" \
		--seed-mbtiles="$(STREAM_SEED_MBTILES)" \
		--shard-dir="$(STREAM_SHARD_DIR)" \
		--target-tiles-per-shard="$(STREAM_TARGET_TILES_PER_SHARD)" \
		--postserve-endpoints="auto" \
		--worker-count="$(STREAM_PARALLEL_WORKERS)" \
		--commit-interval="$(STREAM_COMMIT_INTERVAL)" \
		--request-timeout="$(STREAM_REQUEST_TIMEOUT)" \
		--max-request-timeout="$(STREAM_REQUEST_TIMEOUT_MAX)" \
		--timeout-retries="$(STREAM_TIMEOUT_RETRIES)" \
		--timeout-retry-factor="$(STREAM_TIMEOUT_RETRY_FACTOR)" \
		--postgres-timeout-buffer-ms="$(STREAM_POSTGRES_TIMEOUT_BUFFER_MS)" \
		--dashboard-refresh-ms="$(STREAM_DASHBOARD_REFRESH_MS)" \
		$(STREAM_FLAGS)

merge-mbtiles-shards:
	$(call require_stream_zoom_range)
	$(PYTHON) ./mbtiles/export.py \
		--bbox="$(STREAM_BBOX)" \
		--min-zoom="$(STREAM_MIN_ZOOM)" \
		--max-zoom="$(STREAM_MAX_ZOOM)" \
		--final-mbtiles="$(STREAM_FINAL_MBTILES)" \
		--seed-mbtiles="$(STREAM_SEED_MBTILES)" \
		--shard-dir="$(STREAM_SHARD_DIR)" \
		--target-tiles-per-shard="$(STREAM_TARGET_TILES_PER_SHARD)" \
		--worker-count="$(STREAM_PARALLEL_WORKERS)" \
		--commit-interval="$(STREAM_COMMIT_INTERVAL)" \
		--request-timeout="$(STREAM_REQUEST_TIMEOUT)" \
		--max-request-timeout="$(STREAM_REQUEST_TIMEOUT_MAX)" \
		--timeout-retries="$(STREAM_TIMEOUT_RETRIES)" \
		--timeout-retry-factor="$(STREAM_TIMEOUT_RETRY_FACTOR)" \
		--postgres-timeout-buffer-ms="$(STREAM_POSTGRES_TIMEOUT_BUFFER_MS)" \
		--dashboard-refresh-ms="$(STREAM_DASHBOARD_REFRESH_MS)" \
		--merge-only \
		$(STREAM_FLAGS)

verify-mbtiles:
	$(PYTHON) -c "import os, sqlite3, sys; p = os.path.abspath('$(STREAM_MBTILES)'); print('mbtiles:', p); exists = os.path.exists(p); print('size:', os.path.getsize(p) if exists else 'missing'); exists or sys.exit(1); con = sqlite3.connect(p); print('metadata:', con.execute('select count(*) from metadata').fetchone()[0]); print('tiles:', con.execute('select count(*) from tiles').fetchone()[0]); print('zoom_levels:'); [print(f'  z{zoom}: {count}') for zoom, count in con.execute('select zoom_level, count(*) from tiles group by zoom_level order by zoom_level')]; con.close()"

verify-mbtiles-parallel:
	$(PYTHON) -c "import json, os, sqlite3, sys; shard_dir = os.path.abspath('$(STREAM_SHARD_DIR)'); manifest = os.path.join(shard_dir, 'manifest.json'); final_path = os.path.abspath('$(STREAM_FINAL_MBTILES)'); print('manifest:', manifest); statuses = {}; exists_manifest = os.path.exists(manifest); print('shard_statuses:', (lambda data: __import__('functools').reduce(lambda acc, shard: (acc.__setitem__(shard['status'], acc.get(shard['status'], 0) + 1) or acc), data['shards'].values(), {}))(json.load(open(manifest))) if exists_manifest else 'missing manifest'); print('mbtiles:', final_path); exists = os.path.exists(final_path); print('size:', os.path.getsize(final_path) if exists else 'missing'); exists or sys.exit(1); con = sqlite3.connect(final_path); print('metadata:', con.execute('select count(*) from metadata').fetchone()[0]); print('tiles:', con.execute('select count(*) from tiles').fetchone()[0]); print('zoom_levels:'); [print(f'  z{zoom}: {count}') for zoom, count in con.execute('select zoom_level, count(*) from tiles group by zoom_level order by zoom_level')]; con.close()"

import-sql-dev:
	docker-compose run --rm import-sql /bin/bash

import-osm-dev:
	docker-compose run --rm import-osm /bin/bash

generate-osm-contours:
	docker-compose run --build --rm generate-osm-contours

prefetch-contours:
	@mkdir -p "$(CONTOUR_HGTDIR)"
	@ee_user=$$(sed -n 's/^EARTHEXPLORER_USER=//p' ./.earthexplorerCredentials 2>/dev/null | tail -n 1); \
	ee_password=$$(sed -n 's/^EARTHEXPLORER_PASSWORD=//p' ./.earthexplorerCredentials 2>/dev/null | tail -n 1); \
	if [ -z "$$ee_user" ]; then ee_user=$$(sed -n 's/^USER=//p' ./.earthexplorerCredentials 2>/dev/null | tail -n 1); fi; \
	if [ -z "$$ee_password" ]; then ee_password=$$(sed -n 's/^PASSWORD=//p' ./.earthexplorerCredentials 2>/dev/null | tail -n 1); fi; \
	export EARTHEXPLORER_USER="$$ee_user"; \
	export EARTHEXPLORER_PASSWORD="$$ee_password"; \
	PHYGHTMAP_DOWNLOAD_ONLY=1 \
	PHYGHTMAP_SOURCE="$(CONTOUR_SOURCE)" \
	PHYGHTMAP_EXTRA_ARGS="$(CONTOUR_FLAGS)" \
	docker-compose run --build --rm generate-osm-contours

generate-contours-parallel:
	@mkdir -p "$(CONTOUR_HGTDIR)"
	@ee_user=$$(sed -n 's/^EARTHEXPLORER_USER=//p' ./.earthexplorerCredentials 2>/dev/null | tail -n 1); \
	ee_password=$$(sed -n 's/^EARTHEXPLORER_PASSWORD=//p' ./.earthexplorerCredentials 2>/dev/null | tail -n 1); \
	if [ -z "$$ee_user" ]; then ee_user=$$(sed -n 's/^USER=//p' ./.earthexplorerCredentials 2>/dev/null | tail -n 1); fi; \
	if [ -z "$$ee_password" ]; then ee_password=$$(sed -n 's/^PASSWORD=//p' ./.earthexplorerCredentials 2>/dev/null | tail -n 1); fi; \
	export EARTHEXPLORER_USER="$$ee_user"; \
	export EARTHEXPLORER_PASSWORD="$$ee_password"; \
	PHYGHTMAP_JOBS="$(CONTOUR_JOBS)" \
	PHYGHTMAP_DOWNLOAD_ONLY=0 \
	PHYGHTMAP_SOURCE="$(CONTOUR_SOURCE)" \
	PHYGHTMAP_EXTRA_ARGS="$(CONTOUR_FLAGS)" \
	docker-compose run --build --rm generate-osm-contours
	@pbf=$$(find ./data -maxdepth 1 -type f -name '*.pbf' -size +0c | head -n 1); \
	if [ -z "$$pbf" ]; then \
		echo "No non-empty contour PBF found in ./data"; \
		exit 1; \
	fi; \
	echo "contour_pbf: $$pbf"; \
	ls -lh "$$pbf"

verify-contour-cache:
	@dir="$(CONTOUR_HGTDIR)"; \
	if [ ! -d "$$dir" ]; then \
		echo "Missing contour cache directory $$dir"; \
		exit 1; \
	fi; \
	files=$$(find "$$dir" -type f | wc -l); \
	if [ "$$files" -eq 0 ]; then \
		echo "Contour cache $$dir is empty"; \
		exit 1; \
	fi; \
	echo "contour_cache: $$dir"; \
	du -sh "$$dir"; \
	echo "files: $$files"; \
	find "$$dir" -maxdepth 2 -type f | sed -n '1,20p'

generate-osm-file-stats:
	@echo stats file $(file) from ${area}
	docker-compose run --rm import-osm /bin/bash -c "cd /import; rm *.txt; osmconvert --out-statistics ${file} > ./osmstat.txt"
	./pbfStats.sh ${area}
