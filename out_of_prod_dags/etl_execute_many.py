from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import sqlalchemy

database = "testdata"
host = "host.docker.internal"
src_mssql_conn_url = f"mssql+pyodbc://sa:Valhalla06978!@{host}:8011/{database}?Driver=ODBC+Driver+17+for+SQL+Server"
dest_mssql_conn_url = f"mssql+pyodbc://sa:Valhalla06978!@{host}:8012/{database}?Driver=ODBC+Driver+17+for+SQL+Server"
dest_postgres_conn_url = f"postgres+psycopg2://testdb:testdb@{host}:8013/{database}"
mssql_schema_name = "dbo"
postgres_schema_name = "public"
table_name = "avstemning_sap_data"


# ============ Operator Functions ============

def first_DAG():
    print("============ DAG START ============")

def etl(db_type):
    # Sqlalchemy engines and connecitons
    src_engine = sqlalchemy.create_engine(src_mssql_conn_url)
    if db_type=="mssql": 
        dest_engine = sqlalchemy.create_engine(globals()["dest_"+db_type+"_conn_url"], echo=False, fast_executemany=True)
        dest_conn = dest_engine.connect()
        dest_conn.execute(f"IF OBJECT_ID('{globals()[db_type+'_schema_name']}.{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
    else:                
        dest_engine = sqlalchemy.create_engine(globals()["dest_"+db_type+"_conn_url"], echo=False, executemany_mode = "batch")
        dest_conn = dest_engine.connect()
        dest_conn.execute(f"DROP TABLE IF EXISTS {table_name};")
        
    for chunk in pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", src_engine, chunksize=10000):
        df = pd.DataFrame(chunk.values, columns=chunk.columns)
        df.to_sql(table_name, dest_engine, if_exists='append', index = False )

    print(f'{len(df)} rows inserted to table {table_name} in destination database')
    # Close connections and dispose engines
    src_engine.dispose()
    dest_conn.close()
    dest_engine.dispose()

def print_results(db_type):
    # Sqlalchemy engines and connecitons
    dest_engine = sqlalchemy.create_engine(globals()["dest_"+db_type+"_conn_url"])

    #df = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", dest_engine)
    df = pd.read_sql(f"SELECT * FROM {globals()[db_type+'_schema_name']}.{table_name}", dest_engine)
    print(f'{len(df)} rows fetched from table {table_name} in destination database')

    # Close connections and dispose engines
    dest_engine.dispose()

def final_DAG():
    print("============ DAG END ============")

# ============ DAG ============

with DAG(
    dag_id='etl_execute_many_dag',
    start_date=datetime(2022, 1, 10),
    schedule_interval=None,
    catchup=False,
    tags=["test"]
) as dag:

    #first_dag_task = BranchPythonOperator(task_id='first_DAG', python_callable=first_DAG)

    first_dag_task = PythonOperator(task_id="first_DAG", python_callable=first_DAG) 

    etl_mssql_task = PythonOperator(task_id="etl_mssql_data", python_callable=etl, op_kwargs={'db_type': "mssql"})
    print_mssql_results_task = PythonOperator(task_id="print_mssql_results", python_callable=print_results, op_kwargs={'db_type': "mssql"})
    
    etl_postgres_task = PythonOperator(task_id="etl_postgres_data", python_callable=etl, op_kwargs={'db_type': "postgres"})
    print_postgres_results_task = PythonOperator(task_id="print_postgres_results", python_callable=print_results, op_kwargs={'db_type': "postgres"})

    final_dag_task = PythonOperator(task_id="final_DAG", python_callable=final_DAG, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS) 

    for dest_db_type in ["mssql", "postgres"]:
        first_dag_task >> locals()["etl_"+dest_db_type+"_task"] >> locals()["print_"+dest_db_type+"_results_task"] >> final_dag_task

