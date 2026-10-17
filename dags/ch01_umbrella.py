"""1장 우산 판매 예측 DAG — Airflow 3 버전.

원본: https://github.com/BasPH/data-pipelines-with-apache-airflow/blob/master/chapter01/dags/01_umbrella.py
바뀐 점: DummyOperator → EmptyOperator, days_ago → 고정 날짜, schedule_interval → schedule
"""

import pendulum

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="01_umbrella",
    description="Umbrella example with EmptyOperators.",
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Seoul"),
    schedule="@daily",
):
    fetch_weather_forecast = EmptyOperator(task_id="fetch_weather_forecast")
    fetch_sales_data = EmptyOperator(task_id="fetch_sales_data")
    clean_forecast_data = EmptyOperator(task_id="clean_forecast_data")
    clean_sales_data = EmptyOperator(task_id="clean_sales_data")
    join_datasets = EmptyOperator(task_id="join_datasets")
    train_ml_model = EmptyOperator(task_id="train_ml_model")
    deploy_ml_model = EmptyOperator(task_id="deploy_ml_model")

    fetch_weather_forecast >> clean_forecast_data
    fetch_sales_data >> clean_sales_data
    [clean_forecast_data, clean_sales_data] >> join_datasets
    join_datasets >> train_ml_model >> deploy_ml_model
