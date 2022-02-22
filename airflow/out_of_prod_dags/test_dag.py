from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import sqlalchemy
import csv
import os

src_mssql_conn_uri = "mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8011/testdata?Driver=ODBC+Driver+17+for+SQL+Server"
dest_mssql_conn_uri = "mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8012/testdata?Driver=ODBC+Driver+17+for+SQL+Server"
dest_postgres_conn_uri = "postgres+psycopg2://testdb:testdb@host.docker.internal:8013/testdata"
mssql_schema_name = "dbo"
postgres_schema_name = "public"
table_name = "avstemning_sap_data"
tmp_table_dump = "/tmp/" + table_name + ".csv"

def write_to_csv(filename, engine):
    with open(filename, 'w') as f:
        writer = csv.writer(f, delimiter='\t')
        test = engine.execute(f"SELECT TOP(1000) * FROM {mssql_schema_name}.{table_name}")
        writer.writerow([i[0] for i in test.cursor.description])
        writer.writerows(test.fetchall())

# ============ Operator Functions ============

def first_DAG():
    print("============ DAG START ============")

def copy_to_database():
    src_engine = sqlalchemy.create_engine(src_mssql_conn_uri, echo=False, use_batch_mode=True)
    src_conn = src_engine.connect()
    mssql_hook = PostgresHook.get_hook(src_conn)

def open_csv():
    df = pd.read_csv (tmp_table_dump)
    print(df)

# ============ DAG ============

default_args = {
    'owner': 'even.wanvik@dfo.no',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 17),
    'email': ['even.wanvik@dfo.no'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': 'None',
}

with DAG(
    dag_id='test_dag', catchup=False, tags=["test"], default_args=default_args
) as dag:


    first_dag_task = PythonOperator(task_id="first_DAG", python_callable=first_DAG) 
    write_source_to_csv_task = PythonOperator(task_id="write_source_to_csv", python_callable=copy_to_database) 

    first_dag_task >> write_source_to_csv_task