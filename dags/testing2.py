from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator

args = {
    'owner': 'Airflow',
    'start_date': datetime(2020, 4, 1),
    'depends_on_past': True,
}

dag = DAG(
    dag_id='scheduler_interval_101',
    schedule_interval='0 2 * * *',
    default_args=args,
    tags=['example']
)

hello_my_task = BashOperator(
    task_id='hello_task',
    bash_command='echo "hello_world"',
    dag=dag,
)