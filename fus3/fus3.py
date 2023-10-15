import asyncio
from fus3.lib import uploader

async def upload_file(local_file_name: str, remote_file_name: str) -> None:
    await uploader.main([local_file_name, remote_file_name])

