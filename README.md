Object Storage with following features:

	- S3-compatible API: This implementation supports core S3 operations like PUT, GET, DELETE, and HEAD for objects, as well as operations for buckets.
	- Filesystem Backend: Filesystem-based backend to store the objects, with the root directory at /mnt/objs-data
	- Object Metadata: Handling object metadata, including size, last modified time, and ETag.
	- Multipart Uploads: Handlers for multipart uploads, which is important for larger files.
	- Error Handling: Implemented error responses that mimic S3's XML error format.
	- Bucket and Object Listing: Implementation can list buckets and objects within buckets.
	- Middleware: Logging middleware to track incoming requests.

