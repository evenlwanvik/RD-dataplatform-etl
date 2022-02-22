from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
import sqlalchemy
import logging

table_name = "agltransact"

def get_latest_id():
    logging.info('Get latest agresso ID')

    


def migrate_delta():
    
    query = f"""
        SELECT * FROM dbo.{table_name} 
            account,
            SUBSTRING(account, 1, 2) as acc_class
            dim_1,
            dim_2,
            client,
            period,
            trans_date,
            agrtid,
            last_updated
        FROM agltransact
        WHERE agrtid > ?
            and last_update > ? 
    """

args = {
    'owner': 'Airflow',
    'start_date': datetime(2022, 2, 21),
    'depends_on_past': False,
}

with DAG(
    dag_id='agltransact_delta_v001', 
    schedule_interval='*/15 * * * *',
    tags=["agltransact", "delta"], 
    catchup=False,
    default_args=args
) as dag:
    first_dag_task = BashOperator(
        task_id="first_DAG", 
        bash_command='echo "First DAG"',
    )
    final_dag_task = BashOperator(
        task_id="final_DAG", 
        bash_command='echo "Final DAG"',
    )

    first_dag_task >> final_dag_task