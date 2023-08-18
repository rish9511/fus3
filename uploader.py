import asyncio
import concurrent.futures
import random
import time
import queue
import json
import os
import sys

import aiofiles
import boto3
from progress_bar import show_progress

session = boto3.Session(profile_name='personal')
s3_client = session.client('s3')

s3_file_name = "test-file-24.tar.gz"

etags = {}
TOTAL_SIZE = 0
TOTAL_UPLOADED = 0
PART_SIZE = 100000000


async def store_etag(etag, part):
    global etags

    etags[str(part)] = {
        'ETag': etag,
        'PartNumber': part
    }


def complete_upload(upload_id):

    upload_complete = True
    _etags_ = []

    for idx in range(1, 5+1):
        _etags_.append(etags[str(idx)])

    try:
        response = s3_client.complete_multipart_upload(
            Bucket="async-data-v1",
            Key=s3_file_name,
            MultipartUpload={
                'Parts': _etags_
            },
            UploadId=upload_id
        )
    except Exception as exc:
        print("Failed to complete multi part upload - {}".format(exc))
        upload_complete = False

    return upload_complete


# define a coroutine to run as a task
def upload_part(chunk, part, upload_id):
    try:
        response = s3_client.upload_part(
            Body=chunk,
            Bucket='async-data-v1',
            Key=s3_file_name,
            PartNumber=part,
            UploadId=upload_id
        )
    except Exception as exc:
        print("Failed to upload part - {}".format(exc))

    else:
        return response['ETag'], part


def initiate_upload(file_name: str) -> str:
    response = s3_client.create_multipart_upload(
        Bucket="async-data-v1",
        Key=file_name
    )
    return response['UploadId']


async def read_and_upload(file_path: str, start_pos: int, end_pos: int, part_number: int, upload_id: str):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        async with aiofiles.open(file_path, 'rb') as fp:
            await fp.seek(start_pos)
            chunk = await fp.read(end_pos - start_pos)
            futures.append(executor.submit(upload_part, chunk, part_number, upload_id))

        for future in concurrent.futures.as_completed(futures):
            etag, part = future.result()
            await store_etag(etag, part)
            uploaded_size = (end_pos - start_pos) + 1
            global TOTAL_UPLOADED

            TOTAL_UPLOADED += uploaded_size

            percent_uploaded = (TOTAL_UPLOADED/TOTAL_SIZE)*100

            show_progress(int(percent_uploaded))


# entry point coroutine
async def main(args: list):
    # create the task coroutine
    # coro = upload_part()
    # # wrap in a task object and schedule execution
    # task = asyncio.create_task(coro)

    # Do some sanity checks on the file
    # Create a multipart upload request

    try:
        global s3_file_name
        local_file_name = args[0]
        s3_file_name = args[1]
    except Exception as exc:
        print("Please provide the required input")
        return

    print("Local file name {} ".format(local_file_name))
    print("Remote file name {} ".format(s3_file_name))

    upload_id = initiate_upload(s3_file_name)
    print("\nUpload Id -> {}\n".format(upload_id))
    # file_path_win = 'C:\\Users\\risha\\Downloads\\debian-11.5.0-amd64-netinst.iso'
    # file_path_linux = '/home/zboon/Downloads/pycharm-community-2023.1.tar.gz'

    # TODO: Divide the file only if file size is greater than 100 MBs

    file_size = os.path.getsize(local_file_name)
    if file_size > 104857600:

        global TOTAL_SIZE
        part_size = 104857599  # 100 MBs
        TOTAL_SIZE = total_size = os.path.getsize(local_file_name)
        total_parts = int(total_size/part_size)

        start = -104857599
        end = 0

        tasks = []

        print("size of the file {}".format(TOTAL_SIZE))
        for i in range(1, total_parts + 1):
            part_number = i
            start_pos = start + part_size
            end_pos = start_pos + part_size

            if i == total_parts:
                end_pos = total_size
            start = start_pos + 1

            tasks.append(read_and_upload(local_file_name, start_pos, end_pos, part_number, upload_id))

        await asyncio.gather(*tasks)

        if complete_upload(upload_id):
            print("\nUpload complete")

    else:
        print("Files with size less than 100 MBs not supported")



if __name__ == '__main__':
    start = time.perf_counter()
    args = sys.argv[1:]
    asyncio.run(main(args))
    total = time.perf_counter() - start
    print('Executed in {} seconds'.format(round(total, 0)))