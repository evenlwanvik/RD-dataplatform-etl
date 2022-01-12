from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import sqlalchemy

src_mssql_conn_uri = "mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8011/ans_testdata?Driver=ODBC+Driver+17+for+SQL+Server"
dest_mssql_conn_uri = "mssql+pyodbc://sa:Valhalla06978!@host.docker.internal:8012/ans_testdata?Driver=ODBC+Driver+17+for+SQL+Server"
dest_postgres_conn_uri = "postgres+psycopg2://testdb:testdb@host.docker.internal:8013/ans_testdata"

def etl():
    # Sqlalchemy engines and connecitons
    src_engine = sqlalchemy.create_engine(src_mssql_conn_uri)
    dest_mssql_engine = sqlalchemy.create_engine(dest_mssql_conn_uri)
    dest_postgres_engine = sqlalchemy.create_engine(dest_postgres_conn_uri)
    src_conn = src_engine.connect()
    dest_mssql_conn = dest_mssql_engine.connect()
    dest_postgres_conn = dest_postgres_engine.connect()

    mssql_schema_name = "dbo"
    postgresl_schema_name = "public"
    table_name = "sample_data"

    # Read source -> remove sensitive data (user id) -> load to both destination dbs
    df_src = pd.read_sql(f"SELECT TOP(20) * FROM {mssql_schema_name}.{table_name}", src_conn)
    df_src.drop('user_id', inplace=True, axis=1)      
    df_src.to_sql(schema=mssql_schema_name, name=table_name, con=dest_mssql_conn, index=False, index_label=False, if_exists='replace')
    df_src.to_sql(schema=postgresl_schema_name, name=table_name, con=dest_postgres_conn, index=False, index_label=False, if_exists='replace')

    df_mssql_dest = pd.read_sql(f"SELECT * FROM {mssql_schema_name}.{table_name}", dest_mssql_conn)
    df_postgres_dest = pd.read_sql(f"SELECT * FROM {postgresl_schema_name}.{table_name}", dest_postgres_conn)

    print("Mssql results:")
    print(df_mssql_dest.columns)
    print("Postgres results:")
    print(df_postgres_dest.columns)

    # Close connections and dispose engines
    src_conn.close()
    dest_mssql_conn.close()
    dest_postgres_conn.close()
    src_engine.dispose()
    dest_mssql_engine.dispose()
    dest_postgres_engine.dispose()

with DAG(
    dag_id='etl_migrate_sample_ans_data',
    start_date=datetime(2022, 1, 10),
    schedule_interval=None,
    catchup=False,
    tags=["example", "test"]
) as dag:

    etl_remove_sensitive_data_task = PythonOperator(task_id="etl_remove_sensitive_data", python_callable=etl)