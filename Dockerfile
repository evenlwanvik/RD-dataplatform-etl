FROM apache/airflow:2.2.3-python3.8

# Login preferred username password set in env file given in docker-compose
# TODO: Use docker secret to further encrypt sensitive user information.
ARG AD_DOMAIN
ARG AD_DOMAIN_USER
ARG AD_DOMAIN_PASSWORD
RUN echo $AD_DOMAIN_PASSWORD | kinit $AD_DOMAIN_USER@$AD_DOMAIN

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

