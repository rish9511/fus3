import asyncio
import boto3
import botocore.exceptions

from fus3.lib import uploader


class Fus3:
    def __init__(self, profile_name=None):
        # self.profile_name = profile_name
        # BotorCoreError and ClientError seems to be the highest level exception classes
        # All other exceptions are mostly based on BotoCoreError. ClientError seems to
        # be the only class that's not based on BotocoreError and instead is a direct
        # subclass of the Exception class
        try:
            self.profile_name = profile_name
            self.session = boto3.Session(self.profile_name)
            self.s3_client = self.session.client('s3')
        except botocore.exceptions.BotoCoreError as exc:
            raise Exception('''Could not establish connection with AWS.
            Make sure the profile name is valid''')

    async def upload_file(self, local_file_name: str, remote_file_name: str) -> None:
        await uploader.main([local_file_name, remote_file_name])

