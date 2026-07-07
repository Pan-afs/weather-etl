# Weather ETL Pipeline with Apache Airflow and PostgreSQL

A simple ETL pipeline built using Apache Airflow, Docker, and PostgreSQL.

The pipeline fetches real-time weather data from Open-Meteo API, transforms the required information, and stores it in PostgreSQL.

---

## Architecture

```
                 Open-Meteo API
                       |
                       |
                       v
              fetch_weather task
                       |
                       |
                    XCom
                       |
                       |
                       v
            transform_weather task
                       |
                       |
                       v
              load_weather task
                       |
                       |
                       v
             PostgreSQL Database
                  weather_data
```

---

# Technologies

- Apache Airflow 2.9.3
- PostgreSQL 16
- Docker
- Docker Compose
- Python
- Open-Meteo Weather API

---

# Project Structure

```
weather/
│
├── dags/
│   └── weather_pipeline.py
│
├── docker-compose.yml
│
├── README.md
│
└── .gitignore
```

---

# ETL Process

## 1. Extract

The `fetch_weather` task requests current weather information from Open-Meteo API.

Example response:

```json
{
  "current": {
    "temperature_2m": 26.5
  }
}
```

The returned JSON is automatically stored in Airflow XCom.

---

## 2. Transform

The `transform_weather` task extracts only the required field:

```
temperature_2m
```

Example:

Input:

```json
{
 "current":{
    "temperature_2m":26.5
 }
}
```

Output:

```
26.5
```

---

## 3. Load

The `load_weather` task inserts the transformed temperature into PostgreSQL.

Database table:

```sql
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    temperature NUMERIC,
    created_at TIMESTAMP DEFAULT NOW()
);
```

Example data:

```
 id | temperature | created_at
----+-------------+---------------------
 1  | 26.8        | 2026-07-07 21:21:35
 2  | 26.5        | 2026-07-07 21:31:36
```

---

# Running the Project

## 1. Start containers

```bash
docker compose up -d
```

Check running containers:

```bash
docker ps
```

Expected:

```
weather-airflow-1
weather-postgres-1
```

---

# Airflow

Open:

```
http://localhost:8080
```

Login:

```
username:
admin

password:
YOUR_PASSWORD
```

---

# Run DAG manually

Enter Airflow container:

```bash
docker exec -it weather-airflow-1 bash
```

List DAGs:

```bash
airflow dags list
```

Run:

```bash
airflow dags test weather_pipeline 2025-01-01
```

Expected:

```
fetch_weather       SUCCESS
transform_weather   SUCCESS
load_weather       SUCCESS
```

---

# PostgreSQL

Connect:

```bash
docker exec -it weather-postgres-1 psql -U weather -d weather
```

Check tables:

```sql
\dt
```

Query data:

```sql
SELECT * FROM weather_data;
```

---

# Docker Configuration

PostgreSQL mapping:

```
Host Port: 5433
Container Port: 5432
```

Connection from host:

```
Host:
localhost

Port:
5433

Database:
weather

Username:
weather

Password:
weather
```

Connection from Airflow container:

```
Host:
weather-postgres-1

Port:
5432

Database:
weather

Username:
weather

Password:
weather
```

---

# Airflow XCom

XCom is Airflow's mechanism for passing small amounts of data between tasks.

Example:

`fetch_weather` returns:

```python
return data
```

Airflow automatically stores this result.

Another task retrieves it:

```python
weather = ti.xcom_pull(
    task_ids="fetch_weather"
)
```

In this project:

```
fetch_weather
       |
       |
       v
    XCom
       |
       |
       v
transform_weather
```

---

# Useful Commands

Stop containers:

```bash
docker compose down
```

Restart:

```bash
docker compose restart
```

View logs:

```bash
docker logs weather-airflow-1
```

---

# Future Improvements

Possible improvements:

- Add Airflow scheduling
- Add data validation checks
- Add retries and failure handling
- Store historical weather data
- Add dashboard using Metabase
- Add CI/CD with GitHub Actions

---

# Author

Weather ETL Pipeline Project