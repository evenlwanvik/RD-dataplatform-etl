import airflow
from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.apache.hive.transfers.mysql_to_hive import MySqlToHiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from pyhive import hive
import pandas as pd
import sqlalchemy

database = "testdata"
host = "host.docker.internal"
mssql_conn_url = f"mssql+pyodbc://sa:Valhalla06978!@{host}:8011/{database}?Driver=ODBC+Driver+17+for+SQL+Server"
hive_conn_url = f"hive://{host}:10000/default"
mssql_schema_name = "dbo"
postgres_schema_name = "public"
table_name = "avstemning_sap_data"

# ============ Tasks ============

def first_DAG():
    print("============ DAG START ============")

def load_mssql_to_hive():
    src_engine = sqlalchemy.create_engine(mssql_conn_url)
    dest_engine =  sqlalchemy.create_engine(hive_conn_url)
    #conn = hive.Connection(host="host.docker.internal", port=10000)

    #for chunk in pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", src_engine, chunksize=10000):
    #    df = pd.DataFrame(chunk.values, columns=chunk.columns)
    #    df.to_sql(table_name, dest_engine, if_exists='append', index=False )

    df = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", src_engine)
    df.to_sql(table_name, dest_engine, if_exists='replace', index=False, chunksize=100, method='multi')

    print(f'{len(df)} rows inserted to table {table_name} in destination database')
    # Close connections and dispose engines
    dest_engine.dispose()
    src_engine.dispose()

def print_results():
    # Sqlalchemy engines and connecitons
    dest_engine = sqlalchemy.create_engine(hive_conn_url)

    #df = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", dest_engine)
    df = pd.read_sql(f"SELECT * FROM {table_name}", dest_engine)
    print(f'{len(df)} rows fetched from table {table_name} in destination database')

    # Close connections and dispose engines
    dest_engine.dispose()


# ============ DAG ============

with DAG(
    dag_id='sql_to_hive',
    start_date=datetime(2022, 1, 10),
    schedule_interval=None,
    catchup=False,
    tags=["test", "hive"]
) as dag:


    first_dag_task = PythonOperator(task_id="first_DAG", python_callable=first_DAG) 

    load_mssql_to_hive_task = PythonOperator(task_id="load_mssql_to_hive", python_callable=load_mssql_to_hive)  
    print_results_task = PythonOperator(task_id="print_results", python_callable=print_results)

    first_dag_task >> load_mssql_to_hive_task >> print_results_task