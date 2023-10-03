import boto3
import sys

import time
import os
import concurrent.futures




session = boto3.Session(profile_name='personal')
s3_client = session.client('s3')

s3_file_name = ""


def initiate_upload(file_name: str) -> str:
    response = s3_client.create_multipart_upload(
        Bucket="async-data-v1",
        Key=file_name
    )
    return response['UploadId']



def upload_part(chunk, part, upload_id):
    try:
        # print("Uploading part {}".format(part))
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
        # store_etag(etag, part)
        # return response['ETag'], part
        return {'PartNumber': part, 'ETag': response['ETag']}


def main(args: list):

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
        chunk_size = 5 * 1024 * 1024

        # Open and upload the file in parallel chunks.
        with open(local_file_name, 'rb') as file:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                part_number = 1
                while True:
                    # Read a chunk of data from the local file.
                    # print("Reading part {}".format(part_number))
                    data = file.read(chunk_size)

                    if not data:
                        break  # No more data to upload.

                    # Submit the upload task to the executor.
                    futures.append(executor.submit(upload_part, data, part_number, upload_id))
                    part_number += 1

                # Collect the results (ETags) of the uploaded parts.
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        etags.append(result)


        etags.sort(key=lambda x: x['PartNumber'])


        # Complete the multipart upload.
        s3_client.complete_multipart_upload(
            Bucket='async-data-v1',
            Key=s3_file_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': etags}
        )
    else:
        print("Files with size less than 100 MBs not supported")


if __name__ == '__main__':
    start = time.perf_counter()
    args = sys.argv[1:]
    main(args)
    total = time.perf_counter() - start
    print('Executed in {} seconds'.format(round(total, 0)))
