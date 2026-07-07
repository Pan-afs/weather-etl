from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests


def fetch_weather():
    url = (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=35.69"
        "&longitude=51.39"
        "&current=temperature_2m"
    )

    response = requests.get(url)
    data = response.json()

    print("Weather data received")
    return data


def extract_temperature(ti):
    weather = ti.xcom_pull(task_ids="fetch_weather")

    print("XCOM DATA:")
    print(weather)

    temperature = weather["current"]["temperature_2m"]

    return temperature


def save_temperature(ti):
    temperature = ti.xcom_pull(task_ids="extract_temperature")

    with open("/tmp/temperature.txt", "w") as f:
        f.write(str(temperature))

    print("Temperature saved")


with DAG(
    dag_id="weather_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
):

    task1 = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather,
    )

    task2 = PythonOperator(
        task_id="extract_temperature",
        python_callable=extract_temperature,
    )

    task3 = PythonOperator(
        task_id="save_temperature",
        python_callable=save_temperature,
    )

    task1 >> task2 >> task3