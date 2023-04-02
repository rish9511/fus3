

Initiating multipart upload

- This is the first request sent to S3 before uploading process begins. AWS returns an _upload ID_ which should be used for each upload request



Uploading an individual part

- Record _part number_ and _ETag_ for each upload - S3 returns ETag value in it's response header 


Completing multipart upload

- The process is finally concluded by sending a `complete multipart upload` request to S3. This request should include 
  the _upload_id_, _part number_ and the _ETag_ 