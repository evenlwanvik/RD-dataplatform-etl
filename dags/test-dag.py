from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator


# TODO: Replace hard-code variable
start_date = datetime(2022, 2, 21)

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


    final_dag_task = BashOperator(
        task_id="final_DAG", 
        bash_command='echo "Final DAG"',
    )

    first_dag_task >> final_dag_task