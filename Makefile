.PHONY: populate-sources run-etl inspect-db

populate-sources:
	@echo "--- Populating all data sources ---"
	docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_oltp_data.py
	docker compose exec airflow-scheduler python /opt/airflow/scripts/generate_tracking_events.py
	docker compose exec airflow-scheduler python /opt/airflow/scripts/seed_minio.py
	@echo "Sources populated successfully!"

run-etl:
	@echo "--- Running ETL pipelines ---"
	docker compose exec airflow-scheduler python -m etl.extract_oltp
	docker compose exec airflow-scheduler python -m etl.extract_json
	docker compose exec airflow-scheduler python -m etl.extract_minio
	@echo "ETL finished successfully!"

generate-plots:
	@echo "--- Generating Analytics Plots ---"
	docker compose exec airflow-scheduler python /opt/airflow/analytics/main.py
	@echo "Plots generated in analytics/plots/"

inspect-db:
	@echo "--- Inspecting DuckDB raw schema ---"
	docker compose exec airflow-scheduler python -c "import duckdb; c=duckdb.connect('/opt/airflow/data/warehouse/logistics.duckdb'); print(c.execute(\"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema='raw'\").fetchall())"

inspect-raw-db:
	@echo "--- Inspecting DuckDB raw schema (Tables, Columns, Types) ---"
	docker compose exec airflow-scheduler python -c "import duckdb; con=duckdb.connect('/opt/airflow/data/warehouse/logistics.duckdb'); df=con.execute(\"SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema='raw' ORDER BY table_name, ordinal_position\").df(); print(df.to_string(index=False))"

inspect-main-db:
	@echo "--- Inspecting DuckDB main schema (dbt models) ---"
	docker compose exec airflow-scheduler python -c "import duckdb; con=duckdb.connect('/opt/airflow/data/warehouse/logistics.duckdb'); df=con.execute(\"SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema='main' ORDER BY table_name, ordinal_position\").df(); print(df.to_string(index=False))"

down-all:
	@echo "--- Destroying infrastructure and wiping data ---"
	docker compose down -v
	rm -rf data/raw/events/*.json
	rm -f data/warehouse/logistics.duckdb
	@echo "Everything is clean!"

serve-docs:
	@echo "--- Generating dbt documentation and lineage graph ---"
	docker compose exec airflow-scheduler bash -c "cd /opt/airflow/dbt_project && dbt docs generate --profiles-dir ."
	@echo "--- Starting local server on port 8081 ---"
	@echo "Open http://localhost:8081 in your browser to see the Lineage Graph."
	@echo "Press Ctrl+C in this terminal to stop the server when you are done."
	cd dbt_project/target && python3 -m http.server 8081