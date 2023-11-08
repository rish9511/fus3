import asyncio
import boto3
import botocore.exceptions
import threading

from fus3.lib import uploader
from fus3.lib.ani import display_network_speed

class Fus3:
    def __init__(self, profile_name=None, max_workers=10):
        # self.profile_name = profile_name
        # BotorCoreError and ClientError seems to be the highest level exception classes
        # All other exceptions are mostly based on BotoCoreError. ClientError seems to
        # be the only class that's not based on BotocoreError and instead is a direct
        # subclass of the Exception class
        try:
            self.profile_name = profile_name
            self.session = boto3.Session(self.profile_name)
            self.s3_client = self.session.client('s3')
            self.max_workers = max_workers
            self.start_network_monitor()
        except botocore.exceptions.BotoCoreError as exc:
            raise Exception('''Could not establish connection with AWS.
            Make sure the profile name is valid''')

    def start_network_monitor(self):
        t = threading.Thread(target=display_network_speed, daemon=True)
        t.start()

    async def upload_file(self, local_file_name: str, remote_file_name: str) -> None:
        await uploader.main([local_file_name, remote_file_name], self.max_workers)
