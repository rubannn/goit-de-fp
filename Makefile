COMPOSE = docker compose
PYTHON = python
WAIT_TIMEOUT = 600
RESULTS_ARGS = --verify
BATCH_ARGS =

help:
	@echo make init     - Create local settings and folders
	@echo make download - Download original CSV datasets
	@echo make run      - Build, start and wait for services
	@echo make seed     - Import CSV into local MySQL
	@echo make topics   - Create required local Kafka topics
	@echo make check    - Verify the environment
	@echo make stream   - Publish source once and process available events
	@echo make results  - Show and verify streaming results
	@echo make test     - Check transformations and sink recovery
	@echo make batch    - Run the three Spark jobs through Airflow
	@echo make batch-check - Verify bronze, silver and gold outputs
	@echo make status   - Show container status
	@echo make logs     - Follow service logs
	@echo make password - Show the Airflow admin password
	@echo make stop     - Stop services and preserve data

.PHONY: help init download run seed topics check stream results test status logs password stop
.PHONY: batch batch-check

batch:
	$(COMPOSE) exec -T airflow python /opt/airflow/project/scripts/run_batch.py $(BATCH_ARGS)

batch-check:
	$(COMPOSE) exec -T airflow spark-submit /opt/airflow/project/scripts/check_batch.py

init:
	$(PYTHON) scripts/setup.py

download: init
	$(PYTHON) scripts/download_data.py

run: init
	$(COMPOSE) up -d --build --wait --wait-timeout $(WAIT_TIMEOUT)

seed:
	$(COMPOSE) exec -T airflow python /opt/airflow/project/scripts/seed_mysql.py

topics:
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic athlete_event_results --partitions 2 --replication-factor 1
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic athlete_avg_stats --partitions 2 --replication-factor 1
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic environment_check --partitions 1 --replication-factor 1

check:
	$(COMPOSE) exec -T airflow spark-submit /opt/airflow/project/scripts/check_environment.py

stream:
	$(COMPOSE) exec -T airflow spark-submit /opt/airflow/project/streaming/mysql_to_kafka.py
	$(COMPOSE) exec -T airflow spark-submit /opt/airflow/project/streaming/streaming_pipeline.py --available-now

results:
	$(COMPOSE) exec -T airflow spark-submit /opt/airflow/project/streaming/inspect_output.py $(RESULTS_ARGS)

test:
	$(COMPOSE) exec -T airflow spark-submit /opt/airflow/project/scripts/check_streaming.py

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail 100

password:
	$(COMPOSE) exec -T airflow cat /opt/airflow/state/standalone_admin_password.txt

stop:
	$(COMPOSE) stop
