import logging
from pathlib import Path
from datetime import datetime

from dag_config import config
from helper_functions import proxy_to_jobdate
from file_loaders import download_files_for_date, convert_data_files, load_files_to_S3

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from has_rows import HasRowsOperator
from s3_to_redshift import S3ToRedshiftOperator
from data_checkers import check_log_files

logger = logging.getLogger(__name__)
logging.basicConfig(level='INFO')
SCHEDULE = None  # '@daily'

redshift_conn_id = 'redshift'

default_args = {
    'owner': 'yt',
    'depends_on_past': False,
    'retries': 0,
    'catchup': False,
    'email_on_retry': False
}

yt_dag = DAG("yt_graph",
             description='Load and transform data in Redshift with Airflow',
             schedule_interval=SCHEDULE,
             start_date=datetime.strptime('080717', config['DATE_FORMAT']),
             default_args=default_args
             )


init_task = DummyOperator(task_id='begin_execution', dag=yt_dag)
end_task = DummyOperator(task_id='end_execution', dag=yt_dag)


def download_files_for_date_wrapper(*args: dict, **kwargs: dict):
    jobdate = proxy_to_jobdate(str(kwargs['execution_date']), default_value=config['DEFAULT_DATE'])
    print(jobdate)
    download_files_for_date(jobdate)


download_files_task = PythonOperator(
    task_id='download_files',
    python_callable=download_files_for_date_wrapper,
    provide_context=True,
    dag=yt_dag)


def convert_data_files_wrapper(*args: dict, **kwargs: dict):
    jobdate = proxy_to_jobdate(str(kwargs['execution_date']), default_value=config['DEFAULT_DATE'])
    print(jobdate)
    convert_data_files(str(Path(config['DATA_PATH'], jobdate)))


convert_data_files_task = PythonOperator(
    task_id='convert_data_files',
    python_callable=convert_data_files_wrapper,
    provide_context=True,
    dag=yt_dag)


def load_files_to_S3_wrapper(*args: dict, **kwargs: dict):
    jobdate = proxy_to_jobdate(str(kwargs['execution_date']), default_value=config['DEFAULT_DATE'])
    print(jobdate)
    load_files_to_S3(str(Path(config['DATA_PATH'], jobdate)))


load_files_to_S3_task = PythonOperator(
    task_id='load_files_to_S3',
    python_callable=load_files_to_S3_wrapper,
    provide_context=True,
    dag=yt_dag)


load_details_from_s3_to_redshift_task = S3ToRedshiftOperator(
    task_id="load_details_from_s3_to_redshift",
    dag=yt_dag,
    table="details",
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id="aws_credentials",
    s3_bucket=config['S3_BUCKET_NAME_DETAILS'],
    s3_key="data/yt_data/details.txt"
)

load_related_from_s3_to_redshift_task = S3ToRedshiftOperator(
    task_id="load_related_from_s3_to_redshift",
    dag=yt_dag,
    table="related",
    redshift_conn_id=redshift_conn_id,
    aws_credentials_id="aws_credentials",
    s3_bucket=config['S3_BUCKET_NAME_RELATED'],
    s3_key="data/yt_data/related_ids.txt"
)


check_norows_details_task = HasRowsOperator(
    task_id="check_norows_details",
    dag=yt_dag,
    redshift_conn_id=redshift_conn_id,
    table='details'
)


check_norows_related_task = HasRowsOperator(
    task_id="check_norows_related",
    dag=yt_dag,
    redshift_conn_id=redshift_conn_id,
    table='related'
)


def check_log_files_wrapper(*args: dict, **kwargs: dict):
    jobdate = proxy_to_jobdate(str(kwargs['execution_date']), default_value=config['DEFAULT_DATE'])
    print(jobdate)
    check_log_files(jobdate, redshift_conn_id=redshift_conn_id, table='details')


check_log_files_task = PythonOperator(
    task_id='check_log_files',
    python_callable=check_log_files_wrapper,
    provide_context=True,
    dag=yt_dag)


init_task >> download_files_task >> convert_data_files_task >> load_files_to_S3_task

load_files_to_S3_task >> load_details_from_s3_to_redshift_task >> check_norows_details_task
load_files_to_S3_task >> load_related_from_s3_to_redshift_task >> check_norows_related_task

check_norows_details_task >> check_log_files_task

check_log_files_task >> end_task
check_norows_related_task >> end_task
