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
