FROM apache/airflow:2.2.2
USER root
# Add your personal apt installs under apt-get install
RUN apt-get update \
  && apt-get install -y --no-install-recommends \ 
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

# Install custom Python packages
RUN pip install --no-cache-dir --user apache-airflow-providers-microsoft-mssql

