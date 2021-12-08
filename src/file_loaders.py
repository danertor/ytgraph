"""
Functions used to download and clean files. It uses a date value for downloading and processing specific data.
"""
import os
import shutil
import re
import json
import traceback
import logging
import asyncio
import aiohttp
import aiofiles
import zipfile
import csv
from io import BytesIO
from pathlib import Path


def load_config(config_path: str) -> dict:
    """
    Configuration in JSON form. To be used by the helper functions for formats and directories paths.
    :param config_path:
    :return:
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as fin:
            return json.load(fin)
    except FileNotFoundError as e:
        traceback.print_exc()
        raise Exception(f"Failed to load config file from {config_path}: {str(e)}")


config = load_config(str(Path(r'..\test\appsettings.json')))
logger = logging.getLogger(__name__)


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


def download_files(dir_path: str, urls: list[str]) -> None:
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
            logger.info(f" Download {url}")
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
                logger.info(f"Extracting file {fname}")
                for zipinfo in zipfin.infolist():
                    if zipinfo.filename.endswith('.txt'):
                        date_filename = os.path.basename(zipinfo.filename)
                        async with aiofiles.open(str(Path(zip_dir_path, date_filename)), "wb") as outfile:
                            logger.info(f"Extracting file {date_filename}")
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
    date_format = '%y%m%d'
    try:
        from datetime import datetime
        _ = datetime.strftime(datetime.strptime(date, date_format), date_format)
    except Exception as e:
        raise ValueError(f"Incorrect date format. The date must be {date_format}.")

    # The website has the 2007 year zip files without the '07' preffix. The dates for other years are still YYMMDD.
    date = date[2:] if date.startswith('07') else date
    target_base_uri = config['TARGET_BASE_URI']
    if not target_base_uri.endswith('/'):
        target_base_uri += '/'
    target_urls = [f"{target_base_uri}{date}.zip", ]
    download_files(config['DATA_PATH'], target_urls)


def parse_detail_line_data(line: str) -> list[list[str]]:
    """
    Split a data file into detail and the related ids list.
    :param line:
    :return:
    """
    fields = line.split('\t')
    detail_fields = fields[:config['NUMBER_DETAIL_FIELDS_IN_FILE']]
    related_ids = fields[config['NUMBER_DETAIL_FIELDS_IN_FILE']:]
    return [detail_fields, related_ids]


def convert_data_files(dir_path: str) -> None:
    """
    Extend and flatten a row from the original data file.
    :param dir_path:
    :return:
    """
    if not os.path.isdir(dir_path):
        raise FileNotFoundError(f"The path {dir_path} foes not exists.")
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
    logging.basicConfig(encoding='utf-8', level=logging.INFO)
    download_files_for_date('070302')
    download_files_for_date('080717')
    convert_data_files(str(Path(config['DATA_PATH'], '070302')))
    convert_data_files(str(Path(config['DATA_PATH'], '080717')))
