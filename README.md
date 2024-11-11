Object Backup Server with following features:

	- Supports S3 operations like PUT, GET, DELETE, and HEAD for objects, as well as operations for buckets.
	- Filesystem Backend: Filesystem-based backend to store the objects, with the root directory at /mnt/objsdata
	- Object Metadata: Handling object metadata, including size, last modified time, and ETag.
	- Multipart Uploads: Handlers for multipart uploads, which is important for larger files.
	- Error Handling: Implemented error responses that mimic S3's XML error format.
	- Bucket and Object Listing: Implementation can list buckets and objects within buckets.
	- Middleware: Logging middleware to track incoming requests.

Backup storage to sbs:

sbs init -r s3://mybucket/.repopath  -t objstore --endpoint http://localhost:50051 --access-key-id objstoreadmin --secret-access-key objstoreadmin
sbs backup -r s3://mybucket/.repopath -s /test  -t objstore --endpoint http://localhost:50051  --access-key-id objstoreadmin --secret-access-key objstoreadmin -n 32
sbs backup -r s3://mybucket/.repopath -s /test  -t objstore --endpoint http://localhost:50051  --access-key-id objstoreadmin --secret-access-key objstoreadmin -n 32
sbs backup -r s3://mybucket/.repopath -s /test  -t objstore --endpoint http://localhost:50051  --access-key-id objstoreadmin --secret-access-key objstoreadmin -n 32
sbs restore -r s3://mybucket/.repopath -d /tmp/restore -v -t objstore -i $1 --access-key-id objstoreadmin --secret-access-key objstoreadmin --endpoint http://localhost:50051 
sbs list-backup  -r s3://mybucket/.repopath  -t objstore -access-key-id objstoreadmin --secret-access-key objstoreadmin --endpoint http://localhost:50051  

S3 commands:

export AWS_ACCESS_KEY_ID=objstoreadmin
export AWS_SECRET_ACCESS_KEY=objstoreadmin

aws --endpoint-url http://localhost:50051 s3 mb s3://mybucket
aws --endpoint-url http://localhost:50051 s3 cp /test s3://mybucket/ --recursive
aws --endpoint-url http://localhost:50051 s3 ls s3://mybucket/
aws --endpoint-url http://localhost:50051 s3 rb s3://mybucket

