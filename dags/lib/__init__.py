import sqlalchemy
from airflow.models import DagRun


def mssql_server_connection(
    host, db, driver="ODBC Driver 17 for SQL Server"
    ) -> sqlalchemy.engine.base.Connection:
    """
    Returns a connection to a mssql server (microsoft login)
    """
    conn_url = f"mssql://@{host}/{db}?driver={driver}&trusted_connection=yes"
    engine = sqlalchemy.create_engine(conn_url, fast_executemany=True)

    return engine.connect()
     

def local_db_engine(
    username, password, host, db, port=None, driver="ODBC Driver 17 for SQL Server"
    ) -> sqlalchemy.engine.base.Engine:
    """ 
    Returns an sqlalchemy engine to local mssql instance (username/password login)
    """
    if port is None:
        conn_url = f"mssql://{username}:{password}@{host}/{db}?driver={driver}"
    else:  
        conn_url = f"mssql://{username}:{password}@{host}:{port}/{db}?driver={driver}"
    
    return sqlalchemy.create_engine(conn_url, fast_executemany=True)


def local_db_connection(
    username, password, host, db, port=None, driver="ODBC Driver 17 for SQL Server"
    ) -> sqlalchemy.engine.base.Connection:
    """ 
    Returns a connection to local mssql instance (username/password login)
    """

    if port is None:
        conn_url = f"mssql+pyodbc://{username}:{password}@{host}/{db}?driver={driver}"
    else:  
        conn_url = f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db}?driver={driver}"
    
    print(conn_url, flush=True)
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


LOCAL_DB_CONNECTION = local_db_connection(
    username="sa",
    password="Valhalla06978!",
    host="172.17.0.1",
    port="7000",
    db="master",
    driver="ODBC+Driver+17+for+SQL+Server"
)