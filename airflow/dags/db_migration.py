from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mssql_hook import MsSqlHook

# This is simply a test for migrating an external db to our mssql db running
# in a container in our docker-compose network.

dag_params = {
    'dag_id': 'postgres_yugabyte_migration',
    'start_date':datetime(2021, 12, 16),
    'schedule_interval': timedelta(minutes=1)
}

with DAG(**dag_params) as dag:
    # Get connections to source (local mssql) and destination (mssql container)
    src = MsSqlHook(server="172.17.0.1",
                    user="sa",
                    password="Valhalla06978!",
                    database="AdventureWorksLT2019",
                    port="1433")
    dest = PostgresHook(server="mssql",
                    user="sa",
                    password="Valhalla06978!")
    src_conn = src.get_conn()
    src_cursor = src_conn.cursor()
    dest_conn = dest.get_conn()
    dest_cursor = dest_conn.cursor()

    # Get the largest value of the ID column
    dest_cursor.execute("SELECT MAX(product_id) FROM products;")
    product_id = dest_cursor.fetchone()[0]
    # If no rows in column, set id to 0
    if product_id is None:
        product_id = 0
    # Select all products from source that have a higher product id then what exists in destination
    src_cursor.execute("SELECT * FROM products WHERE product_id > %s", [product_id])
    # Insert those rows into destination
    dest.insert_rows(table="products", rows=src_cursor)

    # Do the same for orders
    dest_cursor.execute("SELECT MAX(order_id) FROM orders;")
    order_id = dest_cursor.fetchone()[0]
    if order_id is None:
        order_id = 0
    src_cursor.execute("SELECT * FROM orders WHERE order_id > %s", [order_id])
    dest.insert_rows(table="orders", rows=src_cursor)