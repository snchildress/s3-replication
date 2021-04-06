import os
from datetime import datetime

import boto3


def run():
    TODAY = datetime.today().strftime('%Y%m%d')
    LOCAL_FILE_NAME = 'tmp.csv.gz'

    extract_session = create_aws_session()
    download_file(extract_session, LOCAL_FILE_NAME, TODAY)

    load_session = create_aws_session(extract=False)
    upload_file(load_session, LOCAL_FILE_NAME, TODAY)


def create_aws_session(extract=True):
    REGION_NAME = 'us-east-1'

    if extract:
        AWS_ACCESS_KEY_ID = os.environ['EXTRACT_AWS_ACCESS_KEY_ID']
        AWS_ACCESS_KEY_SECRET = os.environ['EXTRACT_AWS_ACCESS_KEY_SECRET']
    else:
        AWS_ACCESS_KEY_ID = os.environ['LOAD_AWS_ACCESS_KEY_ID']
        AWS_ACCESS_KEY_SECRET = os.environ['LOAD_AWS_ACCESS_KEY_SECRET']

    return boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_ACCESS_KEY_SECRET,
        region_name=REGION_NAME
    )


def download_file(session, local_file_name, today):
    EXTRACT_BUCKET_NAME = 'sfdv-growp-data'

    s3 = session.resource('s3')
    s3.Object(
        EXTRACT_BUCKET_NAME,
        f'{today}_file_for_gp.csv.gz'
    ).download_file(local_file_name)


def upload_file(session, local_file_name, today):
    LOAD_BUCKET_NAME = 'growprog-schildress-test'
    s3 = session.resource('s3')

    s3.Object(
        LOAD_BUCKET_NAME,
        f'{today}_file_for_gp.csv.gz'
    ).upload_file(local_file_name)


if __name__ == '__main__':
    run()
