from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import pandas as pd
import sqlalchemy
import hashlib
from lib import (
    LOCAL_DB_CONNECTION,
    EXTERNAL_DB_CONNECTION,
    get_most_recent_dag_run,
    mssql_server_connection,
    delete_row
)

# TODO: Replace hard-code variable
start_date = datetime(2022, 2, 21)

def migrate_delta(**kwargs):

    local_table = "agltransact"
    external_table = "agltransact"
    
    local_db_conn = LOCAL_DB_CONNECTION

    last_id = local_db_conn.execute(
        sqlalchemy.text(
            f"""
            SELECT MAX(agrtid) as last_id
            FROM {local_table}
            """
        )
    ).fetchall()[0][0]

    if last_id is None: 
        print(f"{local_table} is empty: terminating task.")
    else: 
        print(f"Last agrtid stored in local database table {local_table}: {last_id}.")

        print(kwargs["dag_run"])

        dag_id = kwargs["dag_run"].run_id
        last_dag_run = get_most_recent_dag_run(dag_id)
        print(f"Most recent dag run: {last_dag_run}")
        if last_dag_run is None:
            last_dag_run = datetime(2022, 3, 2)
            print(f"Setting initial date to {start_date}")
 
        #external_conn = mssql_server_connection(host="AGR-DB17.sfso.no", db="AgrHam_PK01")

        query = f"""
            SELECT
                account,
                SUBSTRING(account, 1, 2) as acc_class,
                dim_1,
                dim_2,
                client,
                period,
                trans_date,
                agrtid,
                last_update
            FROM {external_table} 
            WHERE agrtid > {last_id}
                OR last_update > Convert(datetime, '{last_dag_run}')
        """
        updated_rows = pd.read_sql_query(query, con=EXTERNAL_DB_CONNECTION)

        if updated_rows.empty:
            print(f"No new data from table {external_table} in source database.", flush=True)
        else:
            # Delete rows with duplicate agrtid before insert -
            # in case new last_update with existing agrtid
            agrtid_list = updated_rows["agrtid"].tolist()
            n_updates = len(agrtid_list)
            if len(agrtid_list) > 0:
                # TODO: Deleting single row is super slow, speed it up.
                for i, agrtid in enumerate(agrtid_list):
                    print(f"{i+1}/{n_updates}\tRemoving {agrtid}")
                    delete_row(local_db_conn, local_table, agrtid)
            
            print("Adding updated rows to local db.")

            # TODO: Set to higher limits and chunksize when working on a more robust server
            # I doubt we ever will see more than 10000 rows updated in a table between runs though..
            if len(updated_rows) <= 10000:
                updated_rows.to_sql(
                    local_table, con=local_db_conn, if_exists="append", index=False)
            else:
                updated_rows.to_sql(
                    local_table, con=local_db_conn, if_exists="append", index=False, 
                    chunksize=1000)

            print(f"{n_updates} rows updated for table {local_table}.", flush=True)

        # After we've pulled latest id's and "supposedly" updated rows 
        # we compare hash checksums of both databases.
        # Methods to pinpoint deviating rows are unsure.

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
            FROM {local_table} 
        """
        local_df = pd.read_sql_query(query, con=LOCAL_DB_CONNECTION)

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
            FROM {local_table} 
        """
        external_df = pd.read_sql_query(query, con=EXTERNAL_DB_CONNECTION)

        local_df_hash = hashlib.sha256(local_df.to_json().encode()).hexdigest()
        external_df_hash =hashlib.sha256(external_df.to_json().encode()).hexdigest()    

        print(f"Does hashes match for TOP 10 rows: {local_df_hash==external_df_hash}", flush=True)
    
    
args = {
    'owner': 'Airflow',
    'start_date': start_date,
    'depends_on_past': False,
}

with DAG(
    dag_id='agltransact_delta_v001', 
    #schedule_interval='* */1 * * *',
    schedule_interval=None,
    tags=["agltransact", "delta"], 
    catchup=False,
    default_args=args
) as dag:

    first_dag_task = BashOperator(
        task_id="first_DAG", 
        bash_command='echo "First DAG"',
    )

    migrate_delta_task = PythonOperator(
        task_id="migrate_agltransact_delta", 
        python_callable=migrate_delta
    ) 

    final_dag_task = BashOperator(
        task_id="final_DAG", 
        bash_command='echo "Final DAG"',
    )

    first_dag_task >> migrate_delta_task >> final_dag_task