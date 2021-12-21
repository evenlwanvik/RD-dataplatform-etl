# airflow-etl-project
Exploring the use of airflow in docker containers using docker-compose for ETL pipeline with both backend two postgres db's - one for metadata and one for data storage.

## Containers
---
### Airflow-init
The initialization service that sets up the environment and exits upon completion.

### Airflow-webserver
The webserver is the front end GUI provided to end users. It usually runs on port 8080, but in our network it runs on port 8000 as the first in the range 8000-80##. Here you can manually trigger and see the structure of the DAGs in a graph format + tons of other diagnostics. Other mentionable features are the adding and editing of connections, and looking at the scheduling and duration of tasks.

### Airflow-scheduler
The scheduler takes care of running all the DAGs, making sure they run optimally and in order. The default executor for Airflow is the Sequential Executor, but if we want to scale Airflow to meet our production needs, and with available computing resources, there are other available executor configurations.

### Airflow-worker
The worker executes the tasks given by the scheduler. I believe we are using the Celery Executor, for which the workload can be distributed on multiple celery workers which can run on different machines. We currently only have one default worker, but if our workload demands it, we could expand our workforce.

### Flower
Flower is a web based tool for monitoring and administrating Celery clusters.

### Postgres
Our airflow configuration uses Postgresql as a metadata database

### Redis
A Redis-broker that forwards messages from scheduler to worker.

## Common errors:
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