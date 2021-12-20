from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook
from datetime import datetime, timedelta
import pymssql 
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime(2019, 2, 13),
    'email': ['example@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def test_db(**context):
    with pymssql.connect(server="mssql",
                    user="sa",
                    password="Valhalla06978!",
                    database="AdventureWorks2019") as conn:
                    df = pd.read_sql("SELECT table_name FROM information_schema.tables", conn)
                    print(df)
                    return df

with DAG(
    'run_test_db', 
    default_args=default_args, 
    schedule_interval=None,
    tags=["test"]
) as dag:
    test_database = PythonOperator(
        task_id='test_database',
        python_callable=test_db,
        execution_timeout=timedelta(minutes=3),
        dag=dag,
        provide_context=True,
        op_kwargs={
            'extra_detail': 'nothing'
        })
    
    test_database
