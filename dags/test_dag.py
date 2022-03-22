from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
import sqlalchemy
import hashlib
from lib import (
    local_db_connection,
    get_most_recent_dag_run,
    mssql_server_connection,
    delete_row
)

# TODO: Replace hard-code variable
start_date = datetime(2022, 2, 21)

def test(**kwargs):

    table = "agltransact"

    LOCAL_DB_CONNECTION = local_db_connection(
        username="sa",
        password="Valhalla06978!",
        host="172.17.0.1",
        port="7000",
        db="master",
        driver="ODBC+Driver+17+for+SQL+Server"
    )

    query = f"""
        SELECT TOP(10)
            account,
            SUBSTRING(account, 1, 2) as acc_class,
            dim_1,
            dim_2,
            client,
            period,
            trans_date,
            agrtid,
            last_update
        FROM {table} 
    """
    updated_rows = pd.read_sql_query(query, con=LOCAL_DB_CONNECTION)
    print(updated_rows, flush=True)
    
args = {
    'owner': 'Airflow',
    'start_date': start_date,
    'depends_on_past': False,
}

with DAG(
    dag_id='test-dag', 
    #schedule_interval='* */1 * * *',
    schedule_interval=None,
    tags=["test", "agltransact"], 
    catchup=False,
    default_args=args
) as dag:

    first_dag_task = BashOperator(
        task_id="first_DAG", 
        bash_command='echo "First DAG"',
    )

    test_task = PythonOperator(
        task_id="test", 
        python_callable=test
    ) 

    final_dag_task = BashOperator(
        task_id="final_DAG", 
        bash_command='echo "Final DAG"',
    )

    first_dag_task >> test_task >> final_dag_task