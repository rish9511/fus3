import asyncio
import random
import time
import queue
import boto3
import json
import os

session = boto3.Session(profile_name='personal')
s3_client = session.client('s3')

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

etags = []


async def store_etag(response, part):
    global etags
    etags.append(
        {
            'ETag': response['ETag'],
            'PartNumber': part
        }
    )


def complete_upload(upload_id):

    upload_complete = True
    try:
        response = s3_client.complete_multipart_upload(
            Bucket="async-data-v1",
            Key="webstorm.tar.gz",
            MultipartUpload={
                'Parts': etags
            },
            UploadId=upload_id
        )
    except Exception as exc:
        print("Failed to complete multi part upload - {}".format(exc))
        upload_complete = False

    return upload_complete


# define a coroutine to run as a task
async def upload_part(chunk, part, upload_id):
    # report a message
    print('Uploading part {} ...'.format(part))
    # sleep a moment
    # time.sleep(2)
    # x = random.randint(1, 20)
    # await asyncio.sleep(x)
    try:
        # await asyncio.sleep(30)
        response = await s3_client.upload_part(
            Body=chunk,
            Bucket='async-data-v1',
            Key='webstorm.tar.gz',
            PartNumber=part,
            UploadId=upload_id
        )
    except Exception as exc:
        print("Failed to upload part - {}".format(exc))

    else:
        print("Uploaded part {} of size {}".format(part, random.randint(1, 100)))
        await store_etag(response, part)
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
    print("Uploaded part {} of size {}".format(part, random.randint(1, 100)))
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


async def read_and_upload(file_path: str, start_pos: int, end_pos: int, part_number: int, upload_id: str):
    print('Reading part {}...'.format(part_number))
    with open(file_path, 'rb') as fp:
        fp.seek(start_pos)
        chunk = fp.read(end_pos - start_pos)
        await upload_part(chunk, part_number, upload_id)


# entry point coroutine
async def main():
    # create the task coroutine
    # coro = upload_part()
    # # wrap in a task object and schedule execution
    # task = asyncio.create_task(coro)

    # Do some sanity checks on the file
    # Create a multi-part upload request

    upload_id = initiateUpload("webstorm.tar.gz")
    # print('Upload id -> {}'.format(upload_id))
    file_path_win = 'C:\\Users\\risha\\Downloads\\debian-11.5.0-amd64-netinst.iso'
    file_path_linux = '/home/grishabh/Downloads/WebStorm-2022.1.2.tar.gz'


    part_size = 100000000
    total_size = os.path.getsize(file_path_linux)
    print('Total size of the file = {}'.format(total_size))
    print('Total parts = {}'.format(total_size/part_size))
    total_parts = int(total_size/part_size)

    start = -100000000
    end = 0


    tasks = []

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
        print("Upload complete")

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
