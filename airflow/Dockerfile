FROM apache/airflow:2.2.3-python3.8
USER root
# Add your personal apt installs under apt-get install
RUN apt-get update \
  && apt-get install -y --no-install-recommends \ 
        vim \
        python-dev \
        gcc \
        g++ \
        libsasl2-dev \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow

