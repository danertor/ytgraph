import boto3
from pathlib import Path

from dag_config import config
from auth import AWS_KEY, AWS_SECRET


def load_files_to_s3(dir_path: str) -> None:
    combined_details_filename = 'details.txt'
    related_ids_filename = 'related_ids.txt'
    combined_details_filepath = str(Path(dir_path, combined_details_filename))
    related_ids_filepath = str(Path(dir_path, related_ids_filename))
    s3_bucket_name_details = config['S3_BUCKET_NAME_DETAILS']
    s3_bucket_name_related = config['S3_BUCKET_NAME_RELATED']

    client = boto3.client('s3', aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)

    with open(combined_details_filepath, "r") as fin:
        client.upload_fileobj(fin, s3_bucket_name_details, combined_details_filename)

    with open(related_ids_filepath, "r") as fin:
        client.upload_fileobj(fin, s3_bucket_name_related, related_ids_filename)
