from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import sqlalchemy

database = "AgrHam_PK01"
src_conn_url = f"mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:7000/testdata?Driver=ODBC+Driver+17+for+SQL+Server"
#src_conn_url = f"mssql+pyodbc://AGR-DB17.sfso.no/{database}?Driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'" 
dest_conn_url = f"mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8011/{database}?Driver=ODBC+Driver+17+for+SQL+Server"
schema_name = "dbo"
table_name = "agltransact"


# ============ Operator Functions ============

def first_DAG():
    print("============ DAG START ============")

def copy_data():
    # Sqlalchemy engines and connecitons
    src_engine = sqlalchemy.create_engine(src_conn_url)
    dest_engine = sqlalchemy.create_engine(dest_conn_url, echo=False, fast_executemany=True)

    df = pd.read_sql(f"SELECT TOP(10000) FROM {schema_name}.{table_name}", src_engine)
    df.to_sql(table_name, dest_engine, if_exists='replace')

    print(f'{len(df)} rows inserted to table {table_name} in destination database')
    # Close connections and dispose engines
    src_engine.dispose()
    dest_engine.dispose()

def print_results():
    # Sqlalchemy engines and connecitons
    dest_engine = sqlalchemy.create_engine(dest_conn_url)

    #df = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", dest_engine)
    df = pd.read_sql(f"SELECT * FROM {schema_name}.{table_name}", dest_engine)
    print(f'{len(df)} rows fetched from table {table_name} in destination database')

    # Close connections and dispose engines
    dest_engine.dispose()

def final_DAG():
    print("============ DAG END ============")

# ============ DAG ============

default_args = {
    'owner': 'even.wanvik@dfo.no',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 18, 13, 0, 0),
    'description': "Testing 'executemany' for faster insertion into SQL DB",
    'email': ['even.wanvik@dfo.no'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    'schedule_interval': '@hourly',
}

with DAG(
    dag_id='migrate_general_ledger', catchup=False, tags=["test"], default_args=default_args
) as dag:

    first_dag_task = PythonOperator(task_id="first_DAG", python_callable=first_DAG) 
    copy_data_task = PythonOperator(task_id="copy_data", python_callable=copy_data) 
    print_results_task = PythonOperator(task_id="print_results", python_callable=print_results) 
    final_dag_task = PythonOperator(task_id="final_DAG", python_callable=final_DAG)

    first_dag_task >> copy_data_task >> print_results_task >> final_dag_task