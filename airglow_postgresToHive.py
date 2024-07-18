from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pickle
from model_load import load_and_prepare_data
from model_teach import train_and_save_model

dag = DAG(
    'dag_airflow',
    default_args=default_args,
    description='Сбор, агрегация и загрузка данных в DWH, создание модели и ее обучение',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1)
)

start = DummyOperator(task_id='start', dag=dag)

create_hive_table_details = HiveOperator(
    task_id='create_hive_table_details',
    hive_cli_conn_id='hive_default',
    hive_cli_params="""
    CREATE TABLE IF NOT EXISTS bookings_details (
        id BIGINT,
        num_passengers INT,
        sales_channel STRING,
        trip_type STRING,
        purchase_lead BIGINT,
        length_of_stay INT,
        flight_hour INT,
        flight_day STRING,
        route STRING,
        booking_origin STRING,
        wants_extra_baggage BOOLEAN,
        wants_preferred_seat BOOLEAN,
        wants_in_flight_meals BOOLEAN,
        flight_duration DOUBLE,
        booking_complete BOOLEAN
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    """,
    dag=dag
)

save_to_csv_temp = PostgresToCSVOperator(
    task_id='save_to_csv_temp',
    postgres_conn_id='postgres_default',
    sql="SELECT * FROM bookings_details",
    csv_filepath="/tmp/postgres_data.csv",
    dag=dag
)

load_to_hive_details = HiveOperator(
    task_id='load_to_hive_details',
    hive_cli_conn_id='hive_default',
    hive_cli_params=f"LOAD DATA LOCAL INPATH '/tmp/postgres_data.csv' OVERWRITE INTO TABLE bookings_details",
    dag=dag
)

# популярные маршруты
create_hive_table_popular_routes = HiveOperator(
    task_id='create_hive_table_popular_routes',
    hive_cli_conn_id='hive_default',
    hive_cli_params="""
    CREATE TABLE IF NOT EXISTS popular_routes  (
        id BIGINT,
        count INT,
        route STRING
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    """,
    dag=dag
)

spark_transform_popular_routes = SparkSubmitOperator(
    task_id='spark_transform_popular_routes',
    application='spark_popular_routes_transformation.py',
    conn_id='spark_default',
    dag=dag
)

load_to_hive_popular_routes = HiveOperator(
    task_id='load_to_hive_popular_routes',
    hive_cli_conn_id='hive_default',
    hive_cli_params=f"LOAD DATA LOCAL INPATH '/tmp/popular_routes.csv' OVERWRITE INTO TABLE popular_routes",
    dag=dag
)

# предпочтения клиентов
create_hive_table_preferences = HiveOperator(
    task_id='create_hive_table_preferences',
    hive_cli_conn_id='hive_default',
    hive_cli_params="""
    CREATE TABLE IF NOT EXISTS preferences (
        id BIGINT,
        sales_channel STRING,
        preferred_seat BOOLEAN,
        in_flight_meals BOOLEAN
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    """,
    dag=dag
)

spark_transform_preferences = SparkSubmitOperator(
    task_id='spark_transform_preferences',
    application="spark_preferences_transformations.py",
    name="preferences",
    application_args=[
        "extract_preferences",
        "/tmp/postgres_data.csv",
        "/tmp/preferences.csv"
    ],
    dag=dag
)

load_to_hive_preferences = HiveOperator(
    task_id='load_to_hive_preferences',
    hive_cli_conn_id='hive_default',
    hql="LOAD DATA LOCAL INPATH '/tmp/preferences.csv' OVERWRITE INTO TABLE preferences",
    dag=dag
)


def load_and_prepare_model():
    file_path = '/tmp/postgres_data.csv'
    return load_and_prepare_data(file_path)


def train_and_save_task():
    file_path = '/tmp/postgres_data.csv'
    X_train, X_test, y_train, y_test, scaler = load_and_prepare_data(file_path)
    model_path = '/temp/model.pkl'
    scaler_path = '/temp/scaler.pkl'
    train_and_save_model(X_train, y_train, X_test,
                         y_test, model_path, scaler_path)


load_and_prepare_task = PythonOperator(
    task_id='load_and_prepare_task',
    python_callable=load_and_prepare_model,
    dag=dag,
)

train_and_save_task = PythonOperator(
    task_id='train_and_save_task',
    python_callable=train_and_save_task,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

start >> create_hive_table_details >> save_to_csv_temp >> load_to_hive >> create_hive_table_popular_routes >> create_hive_table_preferences >> \
    [spark_transform_popular_routes, spark_transform_preferences] >> load_to_hive_popular_routes >>  \
    load_to_hive_preferences >> prepare_data_task >> train_model_task >> load_and_prepare_task >> train_and_save_task >> end
