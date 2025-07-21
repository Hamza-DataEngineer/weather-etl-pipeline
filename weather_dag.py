from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
import json
import pandas as pd
from airflow.operators.python import PythonOperator

api_key = 'openweathermap_api_key'
search_city = 'karachi'

def kelvin_to_celsius(kelvin_temp):
    """Convert temperature from Kelvin to Celsius."""
    celsius_temp = int(kelvin_temp - 273.15)
    return celsius_temp


def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    city = data['name']
    description = data['weather'][0]['description']
    tempe_celsius = kelvin_to_celsius(data['main']['temp'])
    feels_like_celsius = kelvin_to_celsius(data['main']['feels_like'])
    min_temp_celsius = kelvin_to_celsius(data['main']['temp_min'])
    max_temp_celsius = kelvin_to_celsius(data['main']['temp_max'])
    pressure = data['main']['pressure']
    humidity = data['main']['humidity']
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_weather_data = {
        "City": city,
        "Description": description,
        "Temperature (C)": tempe_celsius,
        "Feels Like (C)": feels_like_celsius,
        "Min Temp (C)": min_temp_celsius,
        "Max Temp (C)": max_temp_celsius,
        "Pressure": pressure,
        "Humidity": humidity,
        "Time of Record": time_of_record,
        "Sunrise": sunrise_time,
        "Sunset": sunset_time
    }

    transformed_weather_data_list = [transformed_weather_data]
    df = pd.DataFrame(transformed_weather_data_list)
    aws_credentials = {'key': '***********',
                       'secret': '***********',
                       'token': '***********'
                    }
    now = datetime.now()
    dt_string = now.strftime("%d-%m-%Y %H:%M:%S")
    dt_string = (f'current_weather_data_{search_city}_: ' + dt_string)
    df.to_csv(f's3://etl-airflow-airflow-project-bucket/{dt_string}.csv', index=False, storage_options=aws_credentials)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 8),
    'email': ['myemail@domaim.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG('weather_dag',
        default_args=default_args,
        schedule = '@daily',
        catchup=False,) as dag:

        is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint=(f'/data/2.5/weather?q={search_city}&appid={api_key}')
    )

        extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint=(f'/data/2.5/weather?q={search_city}&appid={api_key}'),
        method='GET',
        response_filter=lambda res: json.loads(res.text),
        log_response=True
    )

        transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
