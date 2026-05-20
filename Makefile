.PHONY: up down pipeline charts test clean verify mlflow-ui urls \
        streaming-up streaming-down streaming-logs

# --- Docker (primary workflow) ---
up:
	docker compose up -d jupyter dash mlflow

down:
	docker compose down

streaming-up:
	docker compose --profile streaming up -d

streaming-down:
	docker compose --profile streaming down

streaming-logs:
	docker compose --profile streaming logs -f kafka-producer kafka-consumer

# --- Pipeline ---
pipeline:
	docker compose exec jupyter python -m bigdata_music.pipeline

pipeline-local:
	PYTHONPATH=src python -m bigdata_music.pipeline

# --- Performance charts (no Jupyter needed) ---
charts:
	python scripts/perf_charts.py

# --- Tests ---
test:
	docker compose exec jupyter pytest /src/tests/ -v --cov=/src/bigdata_music --cov-report=term-missing

test-local:
	PYTHONPATH=src pytest tests/ -v --cov=src/bigdata_music --cov-report=term-missing

# --- MLflow ---
mlflow-ui:
	@echo "MLflow UI: http://localhost:5000"
	docker compose up -d mlflow

# --- Data ---
verify:
	python scripts/download_data.py --source verify

# --- Clean ---
clean-delta:
	rm -rf data/bronze data/silver data/gold
	mkdir -p data/bronze data/silver data/gold

# --- Logs ---
logs:
	docker compose logs -f jupyter

# --- URLs ---
urls:
	@echo "Dashboard:       http://localhost:8050"
	@echo "MLflow:          http://localhost:5000"
	@echo "Spark Master UI: http://localhost:8080"
	@echo "Spark App UI:    http://localhost:4040  (only while pipeline runs)"
	@echo "Jupyter:         http://localhost:8888"
