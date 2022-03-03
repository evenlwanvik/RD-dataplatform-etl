# ETL for accounting and finance data at DFØ

This ETL project uses docker-compose to orchestrate a system of containers. In this project we will test procedures and software for extracting, transforming, loading, and providing analytics of buisness data for our clients. 

## Airflow
---

### **Volumes**
Airflow is run within several docker services within the docker-compose system. Some directories are mounted, which means that their contents are synchronized between local environment and the container:
* **./dags** - Contains the DAGs (Directed Acyclig Graph), which is a python file that contains all the tasks you want to run in sequence, organized in a way that reflects their relationships and dependencies.
* **./logs** - Airflow writes logs for tasks in a way that allows to see logs for each task run.
* **./plugins** - Contains our custom plugins. Using Airflow plugins can be a way to customize the Airflow installation to reflect our ecosystem.

### **Airflow compponents**

* **airflow-scheduler** - The scheduler monitors all tasks and DAGs, then triggers the task instances once their dependencies are complete.
* **airflow-webserver** - Front end GUI provided to end users at http://localhost:8000.
* **airflow-worker** - The worker that executes the tasks given by the scheduler.
* **airflow-init** - The initialization service.
* **flower** - The flower app for monitoring the environment.
* **postgres** - The database.
* **redis** - The redis-broker that forwards messages from scheduler to worker.


### Common errors:
---
If you get an error:
```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs/scheduler'
```
it can be caused by ht biund mount, e.g.,:
```
   service_name:
     ...
     volumes:
      - ./airflow/logs:/opt/airflow/logs
```
Fix it by granting permission to the local logs folder, so that airflow container can write logs:
```
chmod -R 777 airflow/logs/
```

## **Jupyter Notebook**
---

A jupyter notebook container is used as the first stage analytics environment, in which we can run tests on the AdventureWorks database stored in the mssql container. 

When the server starts, i.e., the `jupyter` container, you'll get a notification that the notebooks web server can be reached from, e.g.:
```
localhost:8888/?token=2e12afd535c6bed4381fd95bc0cc834573f5b55a78b074da
```
To connect from outside the container simply swap the port with whatever port is used to connect to the docker container, defined in the docker-compose file.

If you are running the container with the root docker-compose file, the connection information mentioned above can be hard to read form the docker-compose output logs. However, you can first get the name of the image and print its logs:
```
docker ps
docker logs <image_name>
```
In our case you will want to replace the port number with the exposed port of the container, which is defined in docker-compose (8001):
```
localhost:8001/?token=...
```

## **Mssql (Microsoft SQL Server)**
---
A Microsoft SQL Server is used as the final procedure, `load`, of the ETL process. Before we migrate data from external sources of data via Airflow, we are using a intermediate database that holds dummy financial data from a made up company called AdventureWorks created by Microsoft. 

The AdventureWorks backup file is loaded upon building the container, and can be used for testing the analytics software. 

To read the content and test the connection to the AdventureWorks db, you can either connect to it through the python mssql interface module shown in `./notebooks/notebooks/AdventureWorks_example.ipynb`, through the connections used in `./airflow/dags/example_mssql_conn.py`, or with Azure Data Studio through localhost and port 8002.
