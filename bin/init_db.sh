#/bin/bash
service postgresql start
su - postgres bash -c "psql  < /opt/airflow/setup_db.sql"
airflow initdb
airflow connections --delete --conn_id airflow_db
airflow connections --add --conn_id airflow_db --conn_uri postgres://airflow:airflow@127.0.0.1:5432/airflow
