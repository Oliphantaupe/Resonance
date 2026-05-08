.PHONY: up down pipeline notebook test clean verify

# --- Docker ---
up:
	docker compose up -d spark-master spark-worker-1 jupyter dash

down:
	docker compose down

streaming-up:
	docker compose --profile streaming up -d

# --- Pipeline ---
pipeline:
	docker compose exec jupyter python /src/bigdata_music/pipeline.py

pipeline-local:
	python -m bigdata_music.pipeline

# --- Notebook ---
notebook:
	@echo "Jupyter running at http://localhost:8888"
	docker compose up -d jupyter

# --- Tests ---
test:
	docker compose exec jupyter pytest /src/tests/ -v

test-local:
	PYTHONPATH=src pytest tests/ -v

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

spark-ui:
	@echo "Spark Master UI: http://localhost:8080"
	@echo "Spark App UI:    http://localhost:4040"
	@echo "Dash:            http://localhost:8050"
	@echo "Jupyter:         http://localhost:8888"
