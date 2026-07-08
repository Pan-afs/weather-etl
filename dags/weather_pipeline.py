from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
import requests


def fetch_weather():

    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=35.69"
        "&longitude=51.39"
        "&current=temperature_2m"
    )

    response = requests.get(
        url,
        timeout=10
    )

    response.raise_for_status()

    return response.json()


def transform_weather(ti):

    weather = ti.xcom_pull(
        task_ids="fetch_weather"
    )

    temperature = weather["current"]["temperature_2m"]
    weather_time = weather["current"]["time"]

    if temperature < -50:
        raise ValueError("Invalid temperature")

    return {
        "temperature": temperature,
        "weather_time": weather_time,
        "city": "Tehran"
    }


def load_weather(ti):

    data = ti.xcom_pull(
        task_ids="transform_weather"
    )

    temperature = data["temperature"]
    weather_time = data["weather_time"]
    city = data["city"]

    hook = PostgresHook(
        postgres_conn_id="weather_postgres"
    )

    hook.run(
        """
        INSERT INTO weather_data
        (
            city,
            temperature,
            weather_time
        )
        VALUES (%s, %s, %s)
        """,
        parameters=(
            city,
            temperature,
            weather_time
        )
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}


with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
):

    extract = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather
    )

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather
    )

    load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather
    )

    extract >> transform >> load