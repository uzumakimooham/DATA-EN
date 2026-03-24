from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_extract(**context):
    data = extract_data()
    context['ti'].xcom_push(key='raw_data', value=data)

def run_transform(**context):
    data = context['ti'].xcom_pull(key='raw_data', task_ids='extract')
    df = transform_data(data)
    context['ti'].xcom_push(key='transformed_data', value=df.to_dict())

def run_load(**context):
    import pandas as pd
    data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    df = pd.DataFrame(data)
    load_data(df)

with DAG(
    dag_id='football_etl_weekly',
    default_args=default_args,
    description='ETL Pipeline ดึงข้อมูล FPL ทุกสัปดาห์',
    schedule_interval='@weekly',   # รันทุกวันอาทิตย์ 00:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['football', 'etl'],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=run_extract,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=run_transform,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=run_load,
    )

    extract >> transform >> load