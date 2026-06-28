up:
	docker compose up -d

down:
	docker compose down -v

# Gold tables are created in Hive Metastore by the Spark jobs.
# Safe to re-run; this only ensures the schema exists and shows visible tables.
register-gold:
	docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.gold WITH (location = 's3://warehouse/gold')"
	docker exec trino trino --execute "SHOW TABLES FROM iceberg.gold"

trino-cli:
	docker exec -it trino trino
