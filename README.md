Object Storage with following features:

	- S3-compatible API: This implementation supports core S3 operations like PUT, GET, DELETE, and HEAD for objects, as well as operations for buckets.
	- Filesystem Backend: Filesystem-based backend to store the objects, with the root directory at /mnt/objs-data
	- Object Metadata: Handling object metadata, including size, last modified time, and ETag.
	- Multipart Uploads: Handlers for multipart uploads, which is important for larger files.
	- Error Handling: Implemented error responses that mimic S3's XML error format.
	- Bucket and Object Listing: Implementation can list buckets and objects within buckets.
	- Middleware: Logging middleware to track incoming requests.

Some areas that might need attention or improvement:

    Directory Handling: Ensure that your PutObject method correctly handles directory creation for nested paths.
    Concurrent Access: Consider implementing locking mechanisms if you expect concurrent access.
    Content Types: You might want to detect or store content types for objects.
    Pagination: For large buckets, implement pagination in the ListObjects operation.
    Security: Implement authentication and authorization if not already done.
    Performance Optimization: For large-scale use, you might need to optimize file operations or consider using a database for metadata.

To further improve and test your implementation, you could:

    Implement more S3 API features like versioning, lifecycle policies, or access control lists.
    Add comprehensive unit and integration tests.
    Benchmark your implementation against real S3 for performance comparison.
    Consider edge cases like very large files, many small files, or deeply nested directory structures.

Overall, successfully using this for backup and restore operations is a great validation of your S3-compatible object store implementation!
