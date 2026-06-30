#!/bin/bash
pip install -r /opt/airflow/requirements.txt
exec "$@"
