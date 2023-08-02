import asyncio
import concurrent.futures
import random
import time
import queue
import json
import os

import aiofiles
import boto3
from progress_bar import show_progress

session = boto3.Session(profile_name='personal')
s3_client = session.client('s3')

s3_file_name = "test-file-24.tar.gz"

# resp_queue = queue.Queue()
#
#
# async def read_responses():
#     while True:
#         if not resp_queue.empty():
#             resp = resp_queue.get()
#             if resp == "FIN":
#                 "All parts have been uploaded. Exiting..."
#                 break
#             print('Uploaded size {}'.format(resp))

etags = {}
TOTAL_SIZE = 0
TOTAL_UPLOADED = 0
PART_SIZE =  100000000

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

    # print("Etags - {}".format(_etags_))

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
    # print('Uploading part {} ...'.format(part))
    try:
        # await asyncio.sleep(30)
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
        # print("Uploaded part {} of size {} and eTag {} ".format(part, random.randint(1, 100), response['ETag']))
        return response['ETag'], part
        # await store_etag(response['ETag'], part)
        # # print(response)


    # await asyncio.sleep(2) # Upload the file. This has to be a non-blocking call.
    # await s3.upload() - For this to work in async manner it's required that s3.upload() is an awaitable
    # report another message
    # add this in queue [(chunk_number, uploaded-size), ...]
    # global  resp_queue
    # resp_queue.put(random.randint(1, 100))


    # await asyncio.sleep(2) # Upload the file. This has to be a non-blocking call.
    # await s3.upload() - For this to work in async manner it's required that s3.upload() is an awaitable
    # report another message
    # add this in queue [(chunk_number, uploaded-size), ...]
    # print("Uploaded part {} of size {}".format(part, random.randint(1, 100)))
    # global  resp_queue
    # resp_queue.put(random.randint(1, 100))


def initiateUpload(file_name: str) -> str:
    response = s3_client.create_multipart_upload(
        Bucket="async-data-v1",
        Key=file_name
    )
    return response['UploadId']


# async def read_file(file_name: str):
    # read file in async manner and the read file in queue(not sure if required)
    # create a task and use asyncio.sleep(0) so that event loop can pick up new task
    #


# [*****************       ]
#
# T1 - 0 : 100
#
# T2   101 : 201
#
# T3   202 : 300
#
#
# total_uploaded = 100 + 100 + 98 = 298/300
#                = 100/300
#                = 201/300
#
# total_uploaded = 100 + 100





async def read_and_upload(file_path: str, start_pos: int, end_pos: int, part_number: int, upload_id: str):
    # print('Reading part {}...'.format(part_number))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        async with aiofiles.open(file_path, 'rb') as fp:
            await fp.seek(start_pos)
            chunk = await fp.read(end_pos - start_pos)
            futures.append(executor.submit(upload_part, chunk, part_number, upload_id))
            # await upload_part(chunk, part_number, upload_id)

        for future in concurrent.futures.as_completed(futures):
            # print(future.result())
            etag, part = future.result()
            await store_etag(etag, part)
            uploaded_size = (end_pos - start_pos) + 1
            global TOTAL_UPLOADED

            TOTAL_UPLOADED += uploaded_size

            percent_uploaded = (TOTAL_UPLOADED/TOTAL_SIZE)*100


            show_progress(int(percent_uploaded))
            # print("Total uploaded {}".format(TOTAL_UPLOADED))
            # print("End pos   {}".format(end_pos))
            # print("Start pos {}".format(start_pos))


            # print("\nUploaded {} %".format((TOTAL_UPLOADED/TOTAL_SIZE)*100))
    # with open(file_path, 'rb') as fp:
    #     fp.seek(start_pos)
    #     chunk = fp.read(end_pos - start_pos)
    #     await upload_part(chunk, part_number, upload_id)

# entry point coroutine
async def main():
    # create the task coroutine
    # coro = upload_part()
    # # wrap in a task object and schedule execution
    # task = asyncio.create_task(coro)

    # Do some sanity checks on the file
    # Create a multipart upload request

    upload_id = initiateUpload(s3_file_name)
    print("\nUpload Id -> {}\n".format(upload_id))
    # print('Upload id -> {}'.format(upload_id))
    file_path_win = 'C:\\Users\\risha\\Downloads\\debian-11.5.0-amd64-netinst.iso'
    file_path_linux = '/home/zboon/Downloads/pycharm-community-2023.1.tar.gz'


    global TOTAL_SIZE
    part_size = 104857599  # 100 MBs
    TOTAL_SIZE = total_size = os.path.getsize(file_path_linux)
    # print('Total size of the file = {}'.format(total_size))
    # print('Total parts = {}'.format(total_size/part_size))
    total_parts = int(total_size/part_size)

    start = -104857599
    end = 0

    # 0 104857600
    # 104857601 209715201
    # 209715202 314572802
    # 314572803 419430403
    # 419430403 576971333

    tasks = []

    print("size of the file {}".format(TOTAL_SIZE))
    for i in range(1, total_parts + 1):
        part_number = i
        start_pos = start + part_size
        end_pos = start_pos + part_size

        if i == total_parts:
            end_pos = total_size
        start = start_pos + 1

        # print('Part number = {}   Start position = {}   End position = {}'.format(part_number, start_pos, end_pos))
        tasks.append(read_and_upload(file_path_linux, start_pos, end_pos, part_number, upload_id))

    await asyncio.gather(*tasks)

    if complete_upload(upload_id):
        print("\nUpload complete")

    # with open('webstorm-2.tar.gz', 'wb') as nfp:
    #     with open(file_path_linux, 'rb') as fp:
    #         for i in range(1, total_parts+1):
    #             part_number = i
    #             start_pos = start + part_size
    #             end_pos = start_pos + part_size
    #
    #             if i == total_parts:
    #                 end_pos = total_size
    #             start = start_pos + 1
    #
    #             print('Part number = {}   Start position = {}   End position = {}'.format(part_number, start_pos, end_pos))
    #             fp.seek(start_pos)
    #             chunk = fp.read(end_pos - start_pos)
    #
    #             nfp.seek(start_pos)
    #             nfp.write(chunk)

            # TODO: Write to a file and check if the new file is same as original file



    # with open(file_path_linux, 'rb') as fp:
    #     part = 0
    #     tasks = []
    #     while True:
    #         # Read 100 MB of file in one go
    #         chunk = fp.read(100000000) # The read call should be non-blocking as well. check aiofiles
    #         part += 1
    #         print("Read part {}".format(part))
    #         if chunk:
    #             task = asyncio.create_task(upload_part(chunk, part, upload_id))
    #             await task
    #             # tasks.append(upload_part(chunk, part, upload_id))
    #             # resp = await upload_part(chunk, part, upload_id)
    #             # store_etag(resp, part)
    #             # print('Uploaded part {} of size {}'.format(part, resp))
    #         else:
    #             break

    # print("Total tasks {}".format(len(asyncio.all_tasks())))
    # await asyncio.gather(*tasks)

    # if complete_upload(upload_id):
    #     print("Upload complete")

    # # suspend a moment to allow the task to run
    # await asyncio.sleep(0)
    # # do other things, like report a message
    # print('Main is doing other things...')
    # # wait for the task to complete
    # await task

# entry point into the program

if __name__ == '__main__':
    start = time.perf_counter()
    asyncio.run(main())
    total = time.perf_counter() - start

    print('Executed in {} seconds'.format(round(total, 0)))



# read part of file
# start uploading it - while this is getting uploaded, read the next part of the file and create a task for uploading it
# store etag
# complete the upload once all parts have been uploaded


# 104857600 +
#