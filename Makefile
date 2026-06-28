up:
	docker compose up -d

down:
	docker compose down -v

# Run once after SilverToGoldJob to make gold tables visible in Trino.
# Safe to re-run — existing registrations are ignored.
register-gold:
	docker exec trino trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.gold"
	docker exec trino trino --execute "CALL iceberg.system.register_table(schema_name=>'gold',table_name=>'ohlcv_5min',table_location=>'s3a://warehouse/gold/ohlcv_5min')" || true
	docker exec trino trino --execute "CALL iceberg.system.register_table(schema_name=>'gold',table_name=>'ohlcv_15min',table_location=>'s3a://warehouse/gold/ohlcv_15min')" || true
	docker exec trino trino --execute "CALL iceberg.system.register_table(schema_name=>'gold',table_name=>'ohlcv_1hour',table_location=>'s3a://warehouse/gold/ohlcv_1hour')" || true
	docker exec trino trino --execute "CALL iceberg.system.register_table(schema_name=>'gold',table_name=>'daily_summary',table_location=>'s3a://warehouse/gold/daily_summary')" || true

trino-cli:
	docker exec -it trino trino
