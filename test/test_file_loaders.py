import sys
from pathlib import Path


def setup_function():
    # Needed relative import through sys for compatibility with the airflow.cfg custom dags_folder in Airflow 1.X
    sys.path.append(str(Path('../src/airflow')))


def test_clean_detail_line_data_empty_list() -> None:
    from dags import config, clean_detail_line_data
    empty_list = []
    assert [] == clean_detail_line_data(empty_list, '_')


def test_clean_detail_line_data() -> None:
    from dags import config, clean_detail_line_data

    detail_row_data_1 = ['_', '_', '1', '_', '_', '_', '_', '_']
    jobdate_1 = '080101'
    result_1 = ['080101', '_', '_', '2007-02-16', '_', '_', '_', '_', '_']
    assert result_1 == clean_detail_line_data(detail_row_data_1, jobdate_1)


if __name__ == '__main__':
    setup_function()
