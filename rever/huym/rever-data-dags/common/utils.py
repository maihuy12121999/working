#
# @author: anhlt
#

import os


def deploy_mode() -> str:
    from airflow.models import Variable
    return Variable.get('MODE', 'development')


def is_test_mode() -> bool:
    mode = deploy_mode()
    return mode != 'production'


def download_s3_folder(s3_folder: str,
                       local_folder: str,
                       aws_access_key_id: str,
                       aws_secret_access_key: str,
                       aws_s3_bucket: str,
                       region_name: str = 'ap-southeast-1'):
    import os
    import boto3
    s3_client = boto3.resource("s3",
                               aws_access_key_id=aws_access_key_id,
                               aws_secret_access_key=aws_secret_access_key,
                               region_name=region_name)

    bucket = s3_client.Bucket(aws_s3_bucket)
    remote_folder = s3_folder if s3_folder.endswith('/') else f'{s3_folder}/'
    print(f"Downloading: {remote_folder}")
    for obj in bucket.objects.filter(Prefix=f'{remote_folder}'):
        target = obj.key if local_folder is None else os.path.join(local_folder, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            print(obj.key)
            continue
        bucket.download_file(obj.key, target)
        print(f"Downloaded {obj.key} to {target}")
    print(f"Downloaded: {s3_folder}")


def delete_file_or_folder(path):
    from pathlib import Path
    from shutil import rmtree
    if Path(path).is_file():
        path.unlink()
    else:
        for path in Path(path).glob("**/*"):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                rmtree(path)


def get_last_modified_time(path: str) -> int:
    if os.path.exists(path):
        stat = os.stat(path)
        return int(stat.st_mtime * 1000)
    else:
        return 0


def is_modified_ago(path: str, elapsed_time: int) -> bool:
    import datetime
    last_modified = get_last_modified_time(path)
    t = int(datetime.datetime.now().timestamp() * 1000) - elapsed_time
    return last_modified <= t


def base64_encode(plain_str: str) -> str:
    import base64
    encoded_bytes = base64.b64encode(plain_str.encode("utf-8"))
    return encoded_bytes.decode("utf-8")


def base64_decode(base64_str: str) -> str:
    import base64
    encoded_bytes = base64.b64decode(base64_str.encode("utf-8"))
    return encoded_bytes.decode("utf-8")
