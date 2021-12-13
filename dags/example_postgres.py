import datetime

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def get_all_pets():
    request = "SELECT * FROM pet;"
    pg_hook = PostgresHook(postgres_conn_id="postgres_datastorage_db")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    sources = cursor.fetchall() # cursor (table pointer) fetch all data from executed request
    for source in sources:
        print("source: {0} - activated: {1}").format(source[0], source[1])
    return sources

'''
def get_birth_date():
    request = "sql/pets_example/fetch_birth_date.sql"
    params = {'begin_date': '2020-01-01', 'end_date': '2020-12-31'}
    pg_hook = PostgresHook(postgres_conn_id="postgres_datastorage_db")
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request, params)
    sources = cursor.fetchall() # cursor (table pointer) fetch all data from executed request
    print(sources)
    return sources  
'''

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2021, 12, 13),
    schedule_interval=None,
    catchup=False
) as dag:
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_datastorage_db",
        sql="sql/pets_example/create_table.sql",
    )
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="postgres_datastorage_db",
        sql="sql/pets_example/populate_table.sql",
    )
    get_all_pets_hook = PythonOperator(task_id="hook_task", python_callable=get_all_pets)

    create_pet_table >> populate_pet_table >> get_all_pets_hook


