import asyncio
from fus3 import fus3


if __name__ == "__main__":
    local_file = "some-file.mp4"  # Location of local file
    remote_file = "remote-file.mp4"  # Name of remote file
    asyncio.run(fus3.upload_file(local_file, remote_file))
