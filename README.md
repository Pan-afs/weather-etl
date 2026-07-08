# Weather ETL Pipeline

## Overview

This project is a simple ETL (Extract, Transform, Load) pipeline built with Apache Airflow, PostgreSQL, Docker, and Grafana.

The pipeline performs the following steps:

1. Extract weather data from the Open-Meteo API
2. Transform the response into a clean structure
3. Validate the temperature value
4. Load the data into PostgreSQL
5. Visualize the stored data in Grafana

---

## Technologies

* Apache Airflow 2.9.3
* PostgreSQL 16
* Grafana
* Docker Compose
* Python

---

## Project Structure

```text
weather/
│
├── dags/
│   └── weather_pipeline.py
│
├── sql/
│   └── init.sql
│
├── docker-compose.yml
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

---

## Setup

### Clone repository

```bash
git clone <repository-url>
cd weather
```

### Create .env file

```env
POSTGRES_USER=weather
POSTGRES_PASSWORD=weather
POSTGRES_DB=weather
```

---

## Start Services

```bash
docker compose up -d
```

Check containers:

```bash
docker ps
```

Expected services:

* Airflow
* PostgreSQL
* Grafana

---

## PostgreSQL

Connect to database:

```bash
docker exec -it weather-postgres-1 psql -U weather -d weather
```

Create table:

```sql
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    temperature NUMERIC,
    weather_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);
```

Check data:

```sql
SELECT * FROM weather_data;
```

---

## Airflow

Open:

```text
http://localhost:8080
```

Login with your Airflow credentials.

List DAGs:

```bash
docker exec -it weather-airflow-1 bash

airflow dags list
```

Run DAG manually:

```bash
airflow dags test weather_pipeline 2025-01-01
```

---

## Grafana

Open:

```text
http://localhost:3000
```

Default credentials:

```text
admin
admin
```

### Add PostgreSQL datasource

Host:

```text
postgres:5432
```

Database:

```text
weather
```

User:

```text
weather
```

Password:

```text
weather
```

---

## Create Dashboard

Use query:

```sql
SELECT
    weather_time AS time,
    temperature
FROM weather_data
ORDER BY weather_time;
```

Visualization:

```text
Time Series
```

---

## ETL Flow

### Extract

Fetches weather data from Open-Meteo API.

### Transform

Extracts:

* city
* temperature
* weather_time

Validates temperature values.

### Load

Stores processed data inside PostgreSQL.

---

## Testing

Run:

```bash
airflow dags test weather_pipeline 2025-01-01
```

Verify:

```sql
SELECT * FROM weather_data;
```

You should see a new weather record inserted.

---

## Future Improvements

* Add retry policies
* Add logging
* Store humidity and wind speed
* Create Grafana dashboards
* Add alerts
* Deploy on cloud infrastructure
* Add CI/CD with GitHub Actions
