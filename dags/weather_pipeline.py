from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import requests
import psycopg2


def fetch_weather():

    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=35.69"
        "&longitude=51.39"
        "&current=temperature_2m"
    )

    response = requests.get(url)
    data = response.json()

    return data


def transform_weather(ti):

    weather = ti.xcom_pull(
        task_ids="fetch_weather"
    )

    temperature = weather["current"]["temperature_2m"]

    return temperature


def load_weather(ti):

    temperature = ti.xcom_pull(
        task_ids="transform_weather"
    )


    conn = psycopg2.connect(
        host="weather-postgres-1",
        database="weather",
        user="weather",
        password="weather"
    )

    cursor = conn.cursor()


    cursor.execute(
        """
        INSERT INTO weather_data (temperature)
        VALUES (%s)
        """,
        (temperature,)
    )


    conn.commit()

    cursor.close()
    conn.close()

    print("Inserted:", temperature)



with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2025,1,1),
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