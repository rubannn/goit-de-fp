COMPOSE = docker compose
PYTHON = python
WAIT_TIMEOUT = 300

# Keep help first: both GNU Make and Embarcadero Make use it by default.
help:
	@echo make init     - Create .env if missing; preserve existing settings
	@echo make build    - Build the Airflow image
	@echo make run      - Build and start services; wait until healthy
	@echo make up       - Start services without rebuilding; wait until healthy
	@echo make status   - Show service status
	@echo make logs     - Follow service logs
	@echo make password - Show the Airflow admin password
	@echo make results  - Show MySQL medal count results
	@echo make stop     - Stop services and keep data
	@echo make down     - Remove containers and keep data volumes
	@echo make clean    - DELETE project data volumes and the local Airflow image

.PHONY: help init build run up status logs password results stop down clean

init:
	$(PYTHON) -c "from pathlib import Path; p = Path('.env'); p.exists() or p.write_bytes(Path('.env.example').read_bytes())"

build: init
	$(COMPOSE) build

run: init
	$(COMPOSE) up -d --build --wait --wait-timeout $(WAIT_TIMEOUT)

up: init
	$(COMPOSE) up -d --wait --wait-timeout $(WAIT_TIMEOUT)

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail 100

password:
	$(COMPOSE) exec -T airflow cat /opt/airflow/state/standalone_admin_password.txt

results:
	$(COMPOSE) exec -T airflow python -c "from airflow.providers.mysql.hooks.mysql import MySqlHook; print(*MySqlHook(mysql_conn_id='goit_mysql_db').get_records('SELECT * FROM olympic_dataset.medal_counts ORDER BY id'), sep='\n')"

stop:
	$(COMPOSE) stop

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down --volumes --rmi local --remove-orphans
