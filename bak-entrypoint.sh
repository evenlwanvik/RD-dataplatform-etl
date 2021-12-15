#!/usr/bin/env bash
airflow initdb
airflow webserver
pip3 install -r requirements.txt