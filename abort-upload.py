import boto3


session = boto3.Session(profile_name='personal')
s3_client = session.client('s3')


# response = s3_client.abort_multipart_upload(
#     Bucket='async-data-v1',
#     Key='pycharm.tar.gz',
#     UploadId='',
# )

response = s3_client.list_multipart_uploads(Bucket="async-data-v1")
print(response)