
import asyncio
import aioboto3
import time

from pathlib import Path


path = Path("/home/grishabh/Downloads/WebStorm-2022.1.2.tar.gz")
bucket = "async-data-v1"
s3_key = "webstorm.tar.gz.v2"

session = aioboto3.Session(profile_name='personal')


async def main():
    async with session.client("s3") as s3:
        try:
            with path.open("rb") as spfp:
                print(f"Uploading {s3_key} to s3")
                await s3.upload_fileobj(spfp, bucket, s3_key)
        except Exception as e:
            print(f"Unable to s3 upload {path} to {s3_key}: {e} ({type(e)})")


if __name__ == '__main__':
    start = time.perf_counter()
    asyncio.run(main())
    total = time.perf_counter() - start
    print('Executed in {} seconds'.format(round(total, 0)))
