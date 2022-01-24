from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import sqlalchemy

src_mssql_conn_url = "mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8011/testdata?Driver=ODBC+Driver+17+for+SQL+Server"
dest_mssql_conn_url = "mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8012/testdata?Driver=ODBC+Driver+17+for+SQL+Server"
dest_postgres_conn_url = "postgres+psycopg2://testdb:testdb@host.docker.internal:8013/testdata"
mssql_schema_name = "dbo"
postgres_schema_name = "public"
table_name = "avstemning_sap_data"

def get_connection(url):
    engine = sqlalchemy.create_engine(url)
    connection = engine.connect()
    return engine, connection

def close_connection(engine, connection):
    connection.close()
    engine.dispose()

# ============ Operator Functions ============

def first_DAG():
    print("============ DAG START ============")

def etl_mssql():
    # Sqlalchemy engines and connecitons
    src_engine = sqlalchemy.create_engine(src_mssql_conn_url)
    dest_engine = sqlalchemy.create_engine(dest_mssql_conn_url)

    # Read source -> remove sensitive data (user id) -> load to both destination dbs
    df_src = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", src_engine)
    df_src.drop('user_id', inplace=True, axis=1)      
    df_src.to_sql(schema=mssql_schema_name, name=table_name, con=dest_engine, index=False, index_label=False, if_exists='replace', chunksize=100, method='multi')
    
    # Close connections and dispose engines
    src_engine.dispose()
    dest_engine.dispose()

def print_mssql_results():
    # Sqlalchemy engines and connecitons
    dest_engine = sqlalchemy.create_engine(dest_mssql_conn_url)

    df = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", dest_engine)

    print("Mssql results:")
    print(df.columns)

    # Close connections and dispose engines
    dest_engine.dispose()

def etl_postgres():
    # Sqlalchemy engines and connecitons
    src_engine = sqlalchemy.create_engine(src_mssql_conn_url)
    dest_engine = sqlalchemy.create_engine(dest_postgres_conn_url)

    # Read source -> remove sensitive data (user id) -> load to both destination dbs
    df_src = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", src_engine)
    df_src.drop('user_id', inplace=True, axis=1)      
    df_src.to_sql(schema=postgres_schema_name, name=table_name, con=dest_engine, index=False, index_label=False, if_exists='replace', chunksize=100000, method='multi')

    # Close connections and dispose engines
    src_engine.dispose()
    dest_engine.dispose()

def print_postgres_results():
    # Sqlalchemy engines and connecitons
    dest_engine = sqlalchemy.create_engine(dest_postgres_conn_url)

    df = pd.read_sql(f"SELECT * FROM {postgres_schema_name}.{table_name}", dest_engine)

    print("Postgres results:")
    print(df.columns)

    # Close connections and dispose engines
    dest_engine.dispose()

"""
def update_database():
    # Sqlalchemy engines and connecitons
    src_engine = sqlalchemy.create_engine(src_mssql_conn_url)
    dest_mssql_engine = sqlalchemy.create_engine(dest_mssql_conn_url)
    dest_postgres_engine = sqlalchemy.create_engine(dest_postgres_conn_url)
    src_conn = src_engine.connect()
    dest_mssql_conn = dest_mssql_engine.connect()
    dest_postgres_conn = dest_postgres_engine.connect()

    # Get cursor
    src_cursor = src_engine.raw_connection().cursor()
    src_cursor.execute("SELECT MAX(product_id) FROM products;")
    id = src_cursor.fetchone()[0]
    print(id)

    # Close connections and dispose engines
    src_conn.close()
    dest_mssql_conn.close()
    dest_postgres_conn.close()
    src_engine.dispose()
    dest_mssql_engine.dispose()
    dest_postgres_engine.dispose()
"""

# ============ DAG ============

with DAG(
    dag_id='etl_single_line_dag',
    start_date=datetime(2022, 1, 10),
    schedule_interval=None,
    catchup=False,
    tags=["test"]
) as dag:

    first_dag_task = PythonOperator(task_id="first_DAG", python_callable=first_DAG) 
    etl_mssql_task = PythonOperator(task_id="etl_mssql_data", python_callable=etl_mssql)
    #print_mssql_results_task = PythonOperator(task_id="print_mssql_results", python_callable=print_mssql_results)
    #etl_postgres_task = PythonOperator(task_id="etl_postgres_data", python_callable=etl_postgres)
    #print_postgres_results_task = PythonOperator(task_id="print_postgres_results", python_callable=print_postgres_results)

    #first_dag_task >> etl_postgres_task
    first_dag_task >> etl_mssql_task

    #first_dag_task 
    #first_dag_task >> etl_mssql_task >> print_mssql_results_task
    #first_dag_task >> etl_postgres_task >> print_postgres_results_task
