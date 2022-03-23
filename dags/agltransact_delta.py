from airflow import DAG
from airflow.models import DagRun
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime
import pandas as pd
import sqlalchemy
import hashlib

AD_DOMAIN = Variable.get("AD_DOMAIN")
AD_DOMAIN_USER = Variable.get("AD_DOMAIN_USER")
AD_DOMAIN_PASSWORD = Variable.get("AD_DOMAIN_PASSWORD")

# TODO: Replace hard-code variable
start_date = datetime(2022, 3, 15)


def mssql_db_conn(
    username=None, password=None, host=None, db=None, port=None, driver="ODBC Driver 17 for SQL Server", trusted_connection=False 
    ) -> sqlalchemy.engine.base.Connection:
    """ 
    Returns a connection to local mssql instance (username/password login)
    """
    if trusted_connection:
        conn_url = f"mssql+pyodbc://@{host}/{db}?driver={driver}&truster_connection=yes"
    else:  
        conn_url = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db}?driver={driver}"
  
    print(f"connecting to {conn_url}")

    engine = sqlalchemy.create_engine(conn_url, fast_executemany=True)

    return engine.connect()


def get_most_recent_dag_run(dag_id) -> list:
    """
    Get last recebt execution date of a DagRun
    TODO: Look for other alternatives to sorting the whole list of dagruns.
    """
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None  


def delete_row(db_conn: sqlalchemy.engine.base.Connection, table: str, agrtid: int) -> None:
    """
    Delete row with given agrtid
    """
    sql = sqlalchemy.text(
            """
            DELETE FROM agltransact
            WHERE agrtid = :agrtid
            """
        )
    db_conn.execute(sql, agrtid=agrtid)


def migrate_delta(**kwargs):

    table = "agltransact"
    
    local_db_conn = mssql_db_conn(
                        username="sa",
                        password="Valhalla06978!",
                        host="172.17.0.1",
                        port="7000",
                        db="master",
                        driver="ODBC+Driver+17+for+SQL+Server"
    )

    last_id = local_db_conn.execute(
        sqlalchemy.text(
            f"""
            SELECT MAX(agrtid) as last_id
            FROM {table}
            """
        )
    ).fetchall()[0][0]

    if last_id is None: 
        print(f"{table} is empty: terminating task.")
    else: 
        print(f"last agrtid stored in local database table {table}: {last_id}.")

        print(kwargs["dag_run"])

        dag_id = kwargs["dag_run"].run_id
        last_dag_run = get_most_recent_dag_run(dag_id)
        print(f"Most recent dag run: {last_dag_run}")
        if last_dag_run is None:
            last_dag_run = datetime(2022, 3, 2)
            print(f"setting initial date to {start_date}")
 
        external_db_conn = mssql_db_conn(host="AGR-DB17.sfso.no", db="AgrHam_PK01", trusted_connection=True)

        query = f"""
            SELECT
                account,
                dim_1,
                dim_2,
                client,
                period,
                trans_date,
                agrtid,
                last_update
            FROM {table} 
            WHERE agrtid > {last_id}
                --OR last_update > Convert(datetime, '{last_dag_run}')
        """
        updated_rows = pd.read_sql_query(query, con=external_db_conn)

        if updated_rows.empty:
            print(f"no new data from table {table} in source database.", flush=True)
        else:
            # Delete rows with duplicate agrtid before insert -
            # in case new last_update with existing agrtid
            agrtid_list = updated_rows["agrtid"].tolist()
            n_updates = len(agrtid_list)
            if len(agrtid_list) > 0:
                # TODO: Deleting single row is super slow, speed it up.
                for i, agrtid in enumerate(agrtid_list):
                    print(f"{i+1}/{n_updates}\tRemoving {agrtid}")
                    delete_row(local_db_conn, table, agrtid)
            
            print("adding updated rows to local db.")

            # TODO: Set to higher limits and chunksize when working on a more robust server
            # I doubt we ever will see more than 10000 rows updated in a table between runs though..
            if len(updated_rows) <= 10000:
                updated_rows.to_sql(
                    table, con=local_db_conn, if_exists="append", index=False)
            else:
                updated_rows.to_sql(
                    table, con=local_db_conn, if_exists="append", index=False, 
                    chunksize=1000)

            print(f"{n_updates} rows updated for table {table}.", flush=True)

        # After we've pulled latest id's and "supposedly" updated rows 
        # we compare hash checksums of both databases.
        # Methods to pinpoint deviating rows are unsure.

        query = f"""
            SELECT TOP(10)
                account,
                dim_1,
                dim_2,
                client,
                period,
                trans_date,
                agrtid,
                last_update
            FROM {table} 
            where agrtid > {last_id}
            ORDER BY agrtid DESC
        """
        local_df = pd.read_sql_query(query, con=local_db_conn)

        query = f"""
            SELECT TOP(100)
                account,
                dim_1,
                dim_2,
                client,
                period,
                trans_date,
                agrtid,
                last_update
            FROM {table} 
            where agrtid > {last_id}
            ORDER BY agrtid DESC
        """
        external_df = pd.read_sql_query(query, con=external_db_conn)

        local_df_hash = hashlib.sha256(local_df.to_json().encode()).hexdigest()
        external_df_hash = hashlib.sha256(external_df.to_json().encode()).hexdigest()    

        print(f"does hashes match for TOP 10 rows: {local_df_hash==external_df_hash}", flush=True)
    
    
args = {
    'owner': 'Airflow',
    'start_date': start_date,
    'depends_on_past': False,
}

with DAG(
    dag_id='agltransact_delta_v002', 
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

    kerberos_init_task = BashOperator(
        task_id="kerberos_init", 
        bash_command=f"""
            echo {AD_DOMAIN_PASSWORD} | kinit {AD_DOMAIN_USER}@{AD_DOMAIN};
            echo "Kerberos initialised;"
            """,
    )

    migrate_delta_task = PythonOperator(
        task_id="migrate_agltransact_delta", 
        python_callable=migrate_delta
    ) 

    final_dag_task = BashOperator(
        task_id="final_DAG", 
        bash_command='echo "Final DAG"',
    )

    first_dag_task >> kerberos_init_task >> migrate_delta_task >> final_dag_task