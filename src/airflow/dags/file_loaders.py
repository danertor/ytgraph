"""
Functions used to download and clean files. It uses a date value for downloading and processing specific data.
"""
import os
import shutil
import re
import logging
import asyncio
import aiohttp
import aiofiles
import zipfile
import csv
import boto3
from typing import List
from io import BytesIO
from pathlib import Path
from datetime import datetime, timedelta
from . import config, AWS_KEY, AWS_SECRET


logger = logging.getLogger(__name__)

logging.basicConfig(level='INFO')


def rename_directories(dir_path: str) -> None:
    """
    Only used for special ocasiones like the 2007 data that has a zip and folder name different
    :param dir_path:
    :return:
    """
    for date_dir_path in [dir[0] for dir in os.walk(dir_path) if os.path.isdir(dir[0])][1:]:
        if re.match(config['DATA_PATH'] + r'\D+\d{4}$', date_dir_path):
            new_dir_path = f"{date_dir_path[0:-4]}07{date_dir_path[-4:]}"
            if os.path.isdir(new_dir_path):
                shutil.rmtree(new_dir_path)
            os.rename(date_dir_path, new_dir_path)


def download_files(dir_path: str, urls: List[str]) -> None:
    """
    Asynchronous download, decompress files.
    :param dir_path:
    :param urls:
    :return:
    """
    os.makedirs(dir_path, exist_ok=True)
    sema = asyncio.BoundedSemaphore(5)

    async def fetch_file(url):
        fname = url.split("/")[-1]
        # The website has the 2007 year zip files without the '07' preffix. The dates for other years are still YYMMDD.
        if fname.startswith('07'):
            fname = fname[2:]
        downloaded_date = os.path.basename(fname).split('.')[0]

        async with sema, aiohttp.ClientSession() as session:
            logger.info("Download %s", url)
            async with session.get(url, ssl=False) as resp:
                assert resp.status == 200
                data = await resp.read()

        async with aiofiles.open(str(Path(dir_path, fname)), "wb") as outfile:
            await outfile.write(data)

        zip_dir_path = str(Path(dir_path, downloaded_date))
        if os.path.isdir(zip_dir_path):
            shutil.rmtree(zip_dir_path)
        os.makedirs(zip_dir_path)
        with BytesIO(data) as fin:
            with zipfile.ZipFile(fin) as zipfin:
                logger.info("Extracting file %s", fname)
                for zipinfo in zipfin.infolist():
                    if zipinfo.filename.endswith('.txt'):
                        date_filename = os.path.basename(zipinfo.filename)
                        async with aiofiles.open(str(Path(zip_dir_path, date_filename)), "wb") as outfile:
                            logger.info("Extracting file %s", date_filename)
                            with zipfin.open(zipinfo) as file_data:
                                await outfile.write(file_data.read())

    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(fetch_file(url)) for url in urls]
    loop.run_until_complete(asyncio.wait(tasks))
    rename_directories(dir_path)


def download_files_for_date(date: str) -> None:
    """
    Kick off function to download and process data for a specific date.
    :param date: date in specific string format.
    :return:
    """
    date_format = config['DATE_FORMAT']
    try:
        _ = datetime.strftime(datetime.strptime(date, date_format), date_format)
    except Exception as e:
        raise ValueError(f"Incorrect date format. The date must be {date_format}.")

    # The website has the 2007 year zip files without the '07' prefix. The dates for other years are still YYMMDD.
    date = date[2:] if date.startswith('07') else date
    target_base_uri = config['TARGET_BASE_URI']
    if not target_base_uri.endswith('/'):
        target_base_uri += '/'
    target_urls = [f"{target_base_uri}{date}.zip", ]
    download_files(config['DATA_PATH'], target_urls)


def parse_detail_line_data(line: str) -> List[List[str]]:
    """
    Split a data file into detail and the related ids list.
    Converts and adds additional fields
    :param line:
    :return:
    """
    fields = line.split('\t')
    detail_fields = fields[:config['NUMBER_DETAIL_FIELDS_IN_FILE']]
    related_ids = fields[config['NUMBER_DETAIL_FIELDS_IN_FILE']:]
    return [detail_fields, related_ids]


def clean_detail_line_data(detail_row: List[str], date: str) -> List[str]:
    """

    :param detail_row: uncleaned detail row
    :param date: job data to be added to data
    :return: a cleaned list of details fields
    """
    if not detail_row:
        print('detail_row:', detail_row)
        return detail_row
    # The age field is an integer number of days between the date when the video was uploaded and Feb.15,
    # 2007 (YouTube's establishment)
    age_field_location = 2
    age_date_format = '%Y-%m-%d'
    age = int(detail_row[age_field_location].strip()) if detail_row[age_field_location].strip() else 0
    new_date = datetime.strptime('2007-02-15', age_date_format) + timedelta(days=age)
    detail_row[age_field_location] = datetime.strftime(new_date, age_date_format)
    return [date, ] + detail_row


def convert_data_files(dir_path: str) -> None:
    """
    Extend and flatten a row from the original data file.
    :param dir_path:
    :return:
    """
    if not os.path.isdir(dir_path):
        raise FileNotFoundError(f"The path {dir_path} does not exists.")
    combined_details_filename = str(Path(dir_path, 'details.txt'))
    related_ids_filename = str(Path(dir_path, 'related_ids.txt'))
    count = 0
    with open(combined_details_filename, 'w', encoding='utf-8', newline='') as dtls_fout:
        details_csv_writer = csv.writer(dtls_fout)
        with open(related_ids_filename, 'w', encoding='utf-8', newline='') as rltd_fout:
            related_ids_csv_writer = csv.writer(rltd_fout)
            for details_filename in [str(Path(dir_path, filename)) for filename in config['DETAILS_FILES']]:
                if not os.path.isfile(details_filename):
                    break
                with open(details_filename, 'r', encoding='utf-8') as fin:
                    while True:
                        count += 1
                        line = fin.readline()
                        line = line[:-1] if line.endswith('\n') else line
                        if not line:
                            break
                        try:
                            detail_fields, related_ids = parse_detail_line_data(line)
                        except Exception as e:
                            raise ValueError(f"Failed to read line {count} on file {details_filename}"
                                             f"\nData: '{line}'"
                                             f"\nError: {str(e)}")
                        details_csv_writer.writerow(detail_fields)
                        for related_id in related_ids:
                            row = [detail_fields[0], related_id]
                            related_ids_csv_writer.writerow(row)


if __name__ == '__main__':
    download_files_for_date('070302')
    download_files_for_date('080717')
    convert_data_files(str(Path(config['DATA_PATH'], '070302')))
    convert_data_files(str(Path(config['DATA_PATH'], '080717')))
