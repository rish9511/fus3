import asyncio
import concurrent.futures
import random
import time
import queue
import json
import os
import sys
from functools import partial

import aiofiles
import boto3

# from progress_bar import show_progress

session = boto3.Session(profile_name='personal')
s3_client = session.client('s3')

s3_file_name = ""

etags = []
TOTAL_SIZE = 0
TOTAL_UPLOADED = 0
PART_SIZE = 100000000


# print("PID = {}".format(os.getpid()))


async def store_etag(etag, part):
    pass
    # global etags
    #
    # etags[str(part)] = {
    #     'ETag': etag,
    #     'PartNumber': part
    # }


def complete_upload(result, upload_id:str):
    global  etags
    etags.sort(key=lambda x: x['PartNumber'])
    try:
        response = s3_client.complete_multipart_upload(
            Bucket="async-data-v1",
            Key=s3_file_name,
            MultipartUpload={
                'Parts': etags
            },
            UploadId=upload_id
        )
    except Exception as exc:
        print("Failed to complete multi part upload - {}".format(exc))

    finally:
        print("\nUpload complete!!")


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
        return {'PartNumber': part, 'ETag': response['ETag']}


def initiate_upload(file_name: str) -> str:
    response = s3_client.create_multipart_upload(
        Bucket="async-data-v1",
        Key=file_name
    )
    return response['UploadId']


async def read_and_upload(file_path: str, part_size: int, upload_id: str):

    async with aiofiles.open(file_path, 'rb') as fp:
        futures = []
        part_number = 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            while True:
                # read the file
                chunk = await fp.read(part_size)

                if not chunk:
                    break

                futures.append(executor.submit(upload_part, chunk, part_number, upload_id))
                part_number += 1

        for future in concurrent.futures.as_completed(futures):
            global etags
            etags.append(future.result())


# entry point coroutine
async def main(args: list):

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

    # TODO: Divide the file only if file size is greater than 100 MBs

    file_size = os.path.getsize(local_file_name)
    if file_size > 104857600:
        etags = []
        part_size = 5 * 1024 * 1024  # 5 MB
        task = asyncio.create_task(read_and_upload(local_file_name, part_size, upload_id))
        task.add_done_callback(partial(complete_upload, upload_id=upload_id))
        await task
    else:
        print("Files with size less than 100 MBs not supported")


# if __name__ == '__main__':
#     start = time.perf_counter()
#     args = sys.argv[1:]
#     asyncio.run(main(args))
#     total = time.perf_counter() - start
#     print('Executed in {} seconds'.format(round(total, 0)))
