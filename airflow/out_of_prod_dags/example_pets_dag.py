import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# This code is obsolete, as we've migrated from postgres to mssql db,
# however, it will stay as I will want to test similar code in the near future.

def get_all_pets():
    request = "SELECT * FROM pet;"
    pg_hook = PostgresHook(postgres_conn_id="datastorage_conn_id")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall() # cursor (table pointer) fetch all data from executed request
    for source in sources:
        print("Id: {0}, Name: {1}, pet-type: {2}, birth-date: {3}, owner: {4}".format(source[0], source[1], source[2], source[3], source[4]))
    return sources


with DAG(
    dag_id="pets_dag",
    start_date=datetime.datetime(2021, 12, 13),
    schedule_interval=None,
    catchup=False,
    tags=["example", "test"]
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="datastorage_conn_id",
        sql="sql/pets_example/create_table.sql",
    )
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="datastorage_conn_id",
        sql="sql/pets_example/populate_table.sql",
    )
    get_all_pets_hook = PythonOperator(task_id="hook_task", python_callable=get_all_pets)

    create_pet_table >> populate_pet_table >> get_all_pets_hook


