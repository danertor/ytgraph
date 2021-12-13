CREATE ROLE airflow;
CREATE DATABASE airflow;
GRANT ALL PRIVILEGES on database airflow to airflow;
ALTER ROLE airflow SUPERUSER;
ALTER ROLE airflow CREATEDB;
ALTER ROLE airflow WITH LOGIN;
ALTER USER airflow with password 'airflow';
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
