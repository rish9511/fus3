import asyncio
from fus3 import fus3


if __name__ == "__main__":
    fus3_client = fus3.Fus3(profile_name='personal')

    local_file = "some-file.mp4"  # Location of local file
    remote_file = "remote-file.mp4"  # Name of remote file

    asyncio.run(fus3_client.upload_file("/home/zboon/satya.mkv", "fu-satya-0.mkv"))