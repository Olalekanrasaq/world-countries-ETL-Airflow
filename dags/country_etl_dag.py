# Import libraries
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import timedelta, datetime
from extract import extract
from transform import transform
from load import insert_data_into_postgres

url = "https://restcountries.com/v3.1/all" 
e_path = "/opt/airflow/dags/template/data.json"  # extracted json from API
t_path = "/opt/airflow/dags/template/cleaned_data.json"  # transformed json file path

default_args = {
    "owner": "Akinkunmi",
    "email": ["olalekanrasaq1331@gmail.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="world_country_info",
    schedule=None,
    start_date=datetime(2025, 5, 16),
    template_searchpath=['/opt/airflow/dags/template'],
    default_args=default_args,
    description="A DAG to fetch world country information and store it in Postgres database"
) as dag:

    # operators : Python Operator and PostgresOperator
    extract_data = PythonOperator(
        task_id="extract_country_data",
        python_callable=extract,
        op_kwargs={"url": url, "e_path":e_path},
    )

    transform_data = PythonOperator(
        task_id="parse_country_info",
        python_callable=transform,
        op_kwargs={"e_path":e_path, "t_path":t_path},
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_worldcountry",
        sql="sql/create_table.sql",
    )

    load_table = PythonOperator(
    task_id="insert_country_info",
    python_callable=insert_data_into_postgres,
    op_kwargs={"t_path":t_path},
)

    # Task pipeline

    extract_data >> transform_data >> create_table >> load_table
