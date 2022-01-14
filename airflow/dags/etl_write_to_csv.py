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

def get_connection(uri):
    engine = sqlalchemy.create_engine(uri)
    connection = engine.connect()
    return engine, connection

def close_connection(engine, connection):
    connection.close()
    engine.dispose()

def write_to_csv(filename, engine):
    with open(filename, 'w') as f:
        writer = csv.writer(f, delimiter='\t')
        cursor = engine.execute("SELECT * FROM mytable")
        writer.writerow([i[0] for i in cursor.description])
        writer.writerows(cursor.fetchall())

# ============ Operator Functions ============

def first_DAG():
    print("============ DAG START ============")

def etl_mssql():
    tmp_table_dump = "/tmp/" + table_name + ".csv"
    with open(tmp_table_dump, "w") as f:
        # Sqlalchemy engines and connecitons
        src_engine = sqlalchemy.create_engine(src_mssql_conn_uri)
        dest_engine = sqlalchemy.create_engine(dest_mssql_conn_uri)
        metadata = sqlalchemy.MetaData()

        table = sqlalchemy.Table(table_name, metadata, schema=mssql_schema_name, autoload=True, autoload_with=src_engine)
        table.create(engine=dest_engine)

        # Close connections and dispose engines
        src_engine.dispose()
        dest_engine.dispose()

    os.remove(tmp_table_dump)

def print_mssql_results():
    # Sqlalchemy engines and connecitons
    dest_engine = sqlalchemy.create_engine(dest_mssql_conn_uri)

    df = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", dest_engine)

    print("Mssql results:")
    print(df.columns)

    # Close connections and dispose engines
    dest_engine.dispose()

# ============ DAG ============

with DAG(
    dag_id='etl_write_to_csv_dag',
    start_date=datetime(2022, 1, 10),
    schedule_interval=None,
    catchup=False,
    tags=["test"]
) as dag:

    first_dag_task = PythonOperator(task_id="first_DAG", python_callable=first_DAG) 
    etl_mssql_task = PythonOperator(task_id="etl_mssql_data", python_callable=etl_mssql)
    print_mssql_results_task = PythonOperator(task_id="print_mssql_results", python_callable=print_mssql_results)
    #etl_postgres_task = PythonOperator(task_id="etl_postgres_data", python_callable=etl_postgres)
    #print_postgres_results_task = PythonOperator(task_id="print_postgres_results", python_callable=print_postgres_results)


    #first_dag_task 
    first_dag_task >> etl_mssql_task >> print_mssql_results_task
    #first_dag_task >> etl_postgres_task >> print_postgres_results_task
