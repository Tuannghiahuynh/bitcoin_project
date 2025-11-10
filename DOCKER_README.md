# Docker Setup for Airflow

## Quick Start

### 1. Build and Start Services

```bash
# On Linux/Mac, set the Airflow user
echo -e "AIRFLOW_UID=$(id -u)" > .env.docker

# Build and start all services
docker-compose up -d
```

### 2. Access Airflow Web UI

- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 3. Access PostgreSQL

- Host: `localhost`
- Port: `5432`
- Database: `bitcoin_db` (for Bitcoin data) or `airflow` (for Airflow metadata)
- User: `postgres` or `airflow`
- Password: `postgres` or `airflow`

## Services

- **airflow-webserver**: Web UI (port 8080)
- **airflow-scheduler**: Task scheduler
- **postgres**: Database for Airflow metadata and Bitcoin data

## Commands

### Start Services
```bash
docker-compose up -d
```

### Stop Services
```bash
docker-compose down
```

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver
```

### Restart Services
```bash
docker-compose restart
```

### Remove All (including volumes)
```bash
docker-compose down -v
```

### Run Airflow CLI Commands
```bash
docker-compose run --rm airflow-cli airflow dags list
docker-compose run --rm airflow-cli airflow dags trigger bitcoin_etl_pipeline
```

## DAG Management

### List DAGs
```bash
docker-compose run --rm airflow-cli airflow dags list
```

### Trigger DAG Manually
```bash
docker-compose run --rm airflow-cli airflow dags trigger bitcoin_etl_pipeline
```

### Pause/Unpause DAG
```bash
docker-compose run --rm airflow-cli airflow dags pause bitcoin_etl_pipeline
docker-compose run --rm airflow-cli airflow dags unpause bitcoin_etl_pipeline
```

## Troubleshooting

### Check Service Status
```bash
docker-compose ps
```

### Check Service Health
```bash
docker-compose ps
# Look for "healthy" status
```

### Access Container Shell
```bash
docker-compose exec airflow-webserver bash
```

### Reset Everything
```bash
docker-compose down -v
docker-compose up -d
```

## Configuration

- **docker-compose.yml**: Service definitions
- **Dockerfile**: Custom Airflow image with project dependencies
- **.env.docker**: Environment variables for Docker
- **init-db.sql**: Database initialization script

## Notes

- DAG runs every 30 minutes automatically
- Bitcoin data stored in `bitcoin_db` database
- Airflow metadata stored in `airflow` database
- Logs saved in `./logs` directory
- Data files saved in `./data` directory
