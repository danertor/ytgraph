import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List
import pytest


mock_log_data = """BFS crawl

video ID, uploader, age, category, length, views, rate, ratings, comments, related ID

start:  010101 00:00:00
finish: 010101 23:59:59

depth	video	time
0	100	67
1	200	519
2	300	2766
3	400	14853
total	10000	18205
last	183493"""
mock_job_date = '010101'
mock_log_file_path = ''


def setup_function():
    # Needed relative import through sys for compatibility with the airflow.cfg custom dags_folder in Airflow 1.X
    sys.path.append(str(Path('../src/airflow')))
    from dags import config

    mock_log_file_path = Path(config['DATA_PATH'], mock_job_date, 'log.txt.')
    mock_log_file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(mock_log_file_path, 'w') as fout:
        fout.write(mock_log_data)


def test_check_log_files() -> None:
    from dags import config, clean_detail_line_data
    empty_list = []
    assert [] == clean_detail_line_data(empty_list, '_')


def teardown_function():
    import shutil
    try:
        if mock_log_file_path:
            shutil.rmtree(mock_log_file_path)
    except Exception as e:
        # This is intentional. Idempotent tear down. do not fail for cleaning up.
        pass


if __name__ == '__main__':
    setup_function()
