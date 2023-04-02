# File size is 10 GB

# Divide file in smaller chunks(check what is the recommended size by AWS for multipart upload)

# Parallely upload the smaller chunks - How will aws recombine the file

'''
  Approach 1 - Break down entire file in smaller chunks. Store the chunks for later processing. Once reading is done,
  upload all the parts in parallel. This would require to store the entire file in memory, given that all the reading
  is done at once. TODO - Need to think more if this can be further optimized

 Approach 2 - Reading a small chunk of file and upload it asynchronously
'''

# def upload_part(chunk, part):
#     pass


# with open("C:\\Users\\risha\\Downloads\\debian-11.5.0-amd64-netinst.iso", "rb") as f:
#     all_chunks = []
#     part = 0
#     while True:
#         chunk = f.read(100000000)
#         part += 1
#         # all_chunks.append(chunk)
#         if not chunk:
#             break
#         print("Part number {} ".format(part))
        # this should happen asynchronously
        # upload_part(chunk, part)
    # print("Total number of chunks - {}".format(len(all_chunks)))

import boto3

session = boto3.Session(profile_name='personal')
s3_client = session.client('s3')

upload_id = None

response = s3_client.create_multipart_upload(
    Bucket="async-data-v1",
    Key="debian.iso"
)

print("Upload id - {} ".format(response['UploadId']))
