from ctypes.wintypes import CHAR
from airflow import DAG
from airflow.models import DagRun
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime
import pandas as pd
import sqlalchemy
import math
import time

AD_DOMAIN = Variable.get("AD_DOMAIN")
AD_DOMAIN_USER = Variable.get("AD_DOMAIN_USER")
AD_DOMAIN_PASSWORD = Variable.get("AD_DOMAIN_PASSWORD")

# TODO: Replace hard-code variable
start_date = datetime(2022, 3, 21)

def mssql_db_conn(
        username=None, 
        password=None, 
        host=None, 
        db=None, 
        port=None, 
        driver="ODBC Driver 17 for SQL Server", 
        trusted_connection=False 
    ) -> sqlalchemy.engine.base.Connection:
    """ 
    Returns a connection to local mssql instance (username/password or mssql)
    """

    if trusted_connection:
        conn_url = f"mssql+pyodbc://@{host}/{db}?driver={driver}&truster_connection=yes"
    else:  
        conn_url = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db}?driver={driver}"

    engine = sqlalchemy.create_engine(conn_url, fast_executemany=True)

    return engine.connect()


def pgsql_server_connection(
    username, password, host, port, db
    ) -> sqlalchemy.engine.base.Connection:
    """
    Returns a connection to a postgresql server (username/password password login)
    """

    conn_url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db}"
    engine = sqlalchemy.create_engine(conn_url, executemany_mode="batch")

    return engine.connect()


def next_month(current_period):
    """
    Input period in yyyymm format and returns next period in yyyymm format
    """
    year, month = int(current_period[:4]), int(current_period[4:])
    month = year*12+month+1 # Next month
    year, month = divmod(month-1, 12)

    return "%d%02d" % (year, month+1) # Final +1 for 12%12 == 0


def migrate_delta():

    initial_period = "201701"
    period = initial_period

    table = "agltransact"

    local_db_conn = pgsql_server_connection(
                        username="admin",
                        password="admin",
                        host="172.17.0.1",
                        port="7000",
                        db="DataPlatform01"
    )

    #sql = sqlalchemy.text(f'DROP TABLE IF EXISTS {table};')
    #local_db_conn.execute(sql)

    try:
        previous_period = pd.read_sql_query(
        f"""
            SELECT
                MAX(period) as last_period
            FROM {table} 
        """, 
        con=local_db_conn
        )["last_period"][0]
        
        period = next_month(str(previous_period))

    except sqlalchemy.exc.SQLAlchemyError:
        print("table does not exist. setting initial period.")
        previous_period = initial_period
        pass

    external_db_conn = mssql_db_conn(host="AGR-DB17.sfso.no", db="AgrHam_PK01", trusted_connection=True)

    n_updated_rows = pd.read_sql_query(
            f"""
                SELECT 
                    count(account) as nRows
                FROM {table} 
                where period = {period}
            """, 
            con=external_db_conn
        )["nRows"][0]

    chunksize = 50000
    wait_time = 20

    print(f"{n_updated_rows} from period {period} for {table} to be migrated to local db", flush=True)
    print(f"waiting {wait_time} seconds between chunks of {chunksize} rows", flush=True)

    query = f"""
        SELECT 
            *
        FROM {table}
        WHERE period = {period}
    """

    for i, chunk in enumerate(pd.read_sql_query(query, con=external_db_conn, chunksize=chunksize)):
        print(f"({i+1}/{math.ceil(n_updated_rows/chunksize)}): Migrating row {i*chunksize} - {i*chunksize+chunksize}")
        df = pd.DataFrame(chunk.values, columns=chunk.columns)
        df.to_sql(table, con=local_db_conn, if_exists='append', index=False, method='multi')
        time.sleep(wait_time)

    print(f"Rows successfully migrated", flush=True)
            
    
args = {
    'owner': 'even.wanvik@dfo.no',
    'start_date': start_date,
    'depends_on_past': False,
}

with DAG(
    dag_id='agltransact_init_v001', 
    schedule_interval='*/15 * * * *',
    tags=["agltransact", "delta"], 
    catchup=False,
    default_args=args
) as dag:

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

    kerberos_init_task >> migrate_delta_task