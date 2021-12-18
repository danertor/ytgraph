"""
Data quality functionality.
"""
import logging
from pathlib import Path
from dag_config import config
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook


logger = logging.getLogger(__name__)

logging.basicConfig(level='INFO')


def check_log_files(date: str, redshift_conn_id: str, table: str) -> None:
    """
    Sanity check if every record was loaded into DB for the Date.
    log files row count from zip must be equal to loaded rowcount into DB for the date.
    :param date: date in specific string format.
    :param redshift_conn_id: The name of the connection string saved in Airflow
    :param table: table name that has the data to be checked.
    :return:
    """
    date_format = config['DATE_FORMAT']
    log_num_records = 0
    try:
        _ = datetime.strftime(datetime.strptime(date, date_format), date_format)
    except Exception as date_exec:
        raise ValueError(f"Incorrect date format. The date must be {date_format}.")
    log_file_path = str(Path(config['DATA_PATH']), date, 'log.txt.')
    # Small file, 15 lines max
    with open(log_file_path, 'r') as fin:
        log_data = fin.read()
    log_data = log_data.split('\n')

    i = 0
    while i < len(log_data):
        line = log_data[i].lower().strip()
        if line.startswith('total') and len(line.split('\t')) > 2:
            log_num_records = line.split('\t')[1]
        i += 1
    if not log_num_records:
        raise ValueError(f"Can not read the total records from log file: {log_file_path}")

    redshift_hook = PostgresHook(redshift_conn_id)
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table} WHERE date='{date}'")
    db_num_records = int(records[0][0])
    assert db_num_records == log_num_records, ValueError(f"Data quality check failed. {table} contained 0 rows")
    logging.info("Data quality on table %s check passed with %s records", table, db_num_records)
