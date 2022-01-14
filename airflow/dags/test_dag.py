from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import sqlalchemy
import csv
import os

src_mssql_conn_uri = "mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8011/ans_testdata?Driver=ODBC+Driver+17+for+SQL+Server"
dest_mssql_conn_uri = "mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8012/ans_testdata?Driver=ODBC+Driver+17+for+SQL+Server"
dest_postgres_conn_uri = "postgres+psycopg2://testdb:testdb@host.docker.internal:8013/ans_testdata"
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

def write_source_to_csv():
    src_engine = sqlalchemy.create_engine(src_mssql_conn_uri)
    write_to_csv(tmp_table_dump, src_engine)

def open_csv():
    df = pd.read_csv (tmp_table_dump)
    print(df)

# ============ DAG ============

with DAG(
    dag_id='test_dag',
    start_date=datetime(2022, 1, 10),
    schedule_interval=None,
    catchup=False,
    tags=["test"]
) as dag:


    first_dag_task = PythonOperator(task_id="first_DAG", python_callable=first_DAG) 
    write_source_to_csv_task = PythonOperator(task_id="write_source_to_csv", python_callable=write_source_to_csv) 

    first_dag_task >> write_source_to_csv_task