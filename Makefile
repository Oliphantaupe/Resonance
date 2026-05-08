.PHONY: up down pipeline notebook test clean verify mlflow-ui urls

# --- Docker ---
up:
	docker compose up -d spark-master spark-worker-1 jupyter dash mlflow

down:
	docker compose down

streaming-up:
	docker compose --profile streaming up -d

# --- Pipeline ---
pipeline:
	docker compose exec jupyter python -m bigdata_music.pipeline

pipeline-local:
	PYTHONPATH=src python -m bigdata_music.pipeline

# --- Notebook ---
notebook:
	@echo "Jupyter running at http://localhost:8888"
	docker compose up -d jupyter

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
	@echo "Spark Master UI: http://localhost:8080"
	@echo "Spark App UI:    http://localhost:4040"
	@echo "Dash:            http://localhost:8050"
	@echo "Jupyter:         http://localhost:8888"
	@echo "MLflow:          http://localhost:5000"
