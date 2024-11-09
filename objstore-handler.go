package main

import (
	"crypto/md5"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

func (s *objStoreServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/xml")

	buckets, err := s.ListBuckets()
	if err != nil {
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		return
	}

	response := struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Xmlns   string   `xml:"xmlns,attr"`
		Owner   struct {
			ID          string `xml:"ID"`
			DisplayName string `xml:"DisplayName"`
		} `xml:"Owner"`
		Buckets struct {
			Bucket []struct {
				Name         string `xml:"Name"`
				CreationDate string `xml:"CreationDate"`
			} `xml:"Bucket"`
		} `xml:"Buckets"`
	}{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
		Owner: struct {
			ID          string `xml:"ID"`
			DisplayName string `xml:"DisplayName"`
		}{
			ID:          "123456789",
			DisplayName: "objStoreServerOwner",
		},
	}

	for _, bucket := range buckets {
		response.Buckets.Bucket = append(response.Buckets.Bucket, struct {
			Name         string `xml:"Name"`
			CreationDate string `xml:"CreationDate"`
		}{
			Name:         bucket.Name,
			CreationDate: bucket.CreationDate.Format(time.RFC3339),
		})
	}

	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
	}
}

/* Process the following commands:
 * Put: Create the bucket
 * Head Get Bucket Status
 * Get: List all files and prefixes under bucket name
 * Delete: Delete the bucket
 */

func (s *objStoreServer) handleBucketCmds(w http.ResponseWriter, r *http.Request) {
	var objects []Object
	var err error

	//printHTTPHeader(r)

	bucketName := mux.Vars(r)["bucket"]

	queryParams := r.URL.Query()
	listType, _ := strconv.Atoi(queryParams.Get("list-type"))
	prefix := queryParams.Get("prefix")
	maxKeys, _ := strconv.Atoi(queryParams.Get("max-keys"))

	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	switch r.Method {
	case "HEAD":
		s.handleHeadCmdBucket(w, r, bucketName)
	case "GET":
		if maxKeys == 0 {
			maxKeys = 1000 // Default value as per S3 API
		}

		if !s.BucketExists(bucketName) {
			s.sendErrorResponse(w, "NoSuchBucket", http.StatusNotFound)
			return
		}

		if listType == 2 {
			objects, err = s.ListObjectsV2(bucketName, prefix, maxKeys)
		} else {
			objects, err = s.ListObjects(bucketName)
		}

		if err != nil {
			log.Printf("Error listing objects: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		response := struct {
			XMLName     xml.Name `xml:"ListBucketResult"`
			Xmlns       string   `xml:"xmlns,attr"`
			Name        string   `xml:"Name"`
			Prefix      string   `xml:"Prefix"`
			KeyCount    int      `xml:"KeyCount"`
			MaxKeys     int      `xml:"MaxKeys"`
			Delimiter   string   `xml:"Delimiter"`
			IsTruncated bool     `xml:"IsTruncated"`
			Contents    []struct {
				Key          string `xml:"Key"`
				LastModified string `xml:"LastModified"`
				ETag         string `xml:"ETag"`
				Size         int64  `xml:"Size"`
				StorageClass string `xml:"StorageClass"`
			} `xml:"Contents"`
			CommonPrefixes []struct {
				Prefix string `xml:"Prefix"`
			}
			EncodingType string `xml:"EncodingType"`
		}{
			Xmlns:        "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:         bucketName,
			Prefix:       prefix,
			MaxKeys:      1000,
			Delimiter:    "/",
			IsTruncated:  false,
			EncodingType: "url",
		}

		for _, obj := range objects {
			if !obj.IsDirectory {
				response.Contents = append(response.Contents, struct {
					Key          string `xml:"Key"`
					LastModified string `xml:"LastModified"`
					ETag         string `xml:"ETag"`
					Size         int64  `xml:"Size"`
					StorageClass string `xml:"StorageClass"`
				}{
					Key:          obj.Key,
					LastModified: formatMinioTime(obj.LastModified),
					ETag:         obj.ETag,
					Size:         obj.Size,
					StorageClass: "STANDARD",
				})
			} else {
				response.CommonPrefixes = append(response.CommonPrefixes, struct {
					Prefix string `xml:"Prefix"`
				}{
					Prefix: obj.Key,
				})
			}
		}
		response.KeyCount = len(response.Contents)

		for i := range response.Contents {
			if strings.HasSuffix(response.Contents[i].Key, "/") {
				response.Contents[i].StorageClass = ""
			}
			// Remove trailing slash from Key if present
			response.Contents[i].Key = strings.TrimSuffix(response.Contents[i].Key, "/")
		}

		// Generate unique IDs for headers
		uniqueID := generateUniqueID()
		requestID := generateUniqueID()

		// Set headers
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/xml")
		w.Header().Set("Server", "MinIO")
		w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		w.Header().Set("Vary", "Origin, Accept-Encoding")
		w.Header().Set("X-Amz-Id-2", uniqueID)
		w.Header().Set("X-Amz-Request-Id", requestID)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Ratelimit-Limit", "333")
		w.Header().Set("X-Ratelimit-Remaining", "333")
		w.Header().Set("X-Xss-Protection", "1; mode=block")
		w.Header().Set("Date", time.Now().UTC().Format(http.TimeFormat))

		// Create XML encoder with indentation
		encoder := xml.NewEncoder(w)
		encoder.Indent("", "  ")

		// Write XML declaration
		w.Write([]byte(xml.Header))

		// Encode and write the response
		if err := encoder.Encode(response); err != nil {
			log.Printf("Error encoding response: %v", err)
			http.Error(w, "Error encoding response", http.StatusInternalServerError)
			return
		}

	case "PUT":
		if !isValidBucketName(bucketName) {
			s.sendErrorResponse(w, "InvalidBucketName", http.StatusBadRequest)
			return
		}

		// Check if this is an ACL operation
		if r.URL.Query().Get("acl") != "" {
			s.PutBucketACLHandler(w, r)
			return
		}

		s.mu.Lock()
		_, exists := s.bucketNames[bucketName]
		if exists {
			s.sendErrorResponse(w, "BucketAlreadyExists", http.StatusBadRequest)
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		// Create bucket
		err := s.CreateBucket(bucketName)
		if err != nil {
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		s.mu.Lock()
		_, exists = s.bucketReplication[bucketName]
		s.mu.Unlock()
		if exists {
			// Create replica bucket
			replicaBucket := bucketName
			err = s.CreateBucketReplica(replicaBucket)
			if err != nil {
				http.Error(w, "Failed to create replica bucket: "+err.Error(), http.StatusInternalServerError)
				return
			}
		}

		s.mu.Lock()
		s.bucketNames[bucketName] = true
		s.mu.Unlock()

		// Add to replication manager
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Location", "/"+bucketName)

	case "DELETE":
		// Delete the original bucket
		err := s.deleteBucket(bucketName)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete bucket %s: %v", bucketName, err), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}

}

/* Process the following commands:
 * Put: Create Object
 * Head: Get Object Status
 * Get: List all files and prefixes under the path
 * Delete: Delete the object
 */
func (s *objStoreServer) handleObjectCmds(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]

	//printHTTPHeader(r)

	if !s.BucketExists(bucketName) {
		s.sendErrorResponse(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	isDirectory := false
	if strings.HasSuffix(objectKey, "/") {
		isDirectory = true
	}

	// Remove the leading slash if it exists
	objectKey = strings.TrimPrefix(objectKey, "/")

	switch r.Method {
	case "HEAD":
		s.handleHeadObject(w, r, bucketName, objectKey)
	case "GET":
		// Get object metadata first
		metadata, err := s.GetObjectMetadata(bucketName, objectKey, r.Header.Get("Content-Type"))
		if err != nil {
			if os.IsNotExist(err) {
				s.sendErrorResponse(w, "NoSuchKey", http.StatusNotFound)
			} else {
				log.Printf("Error getting object metadata: %v", err)
				s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			}
			return
		}

		// Set headers based on metadata
		w.Header().Set("Content-Type", metadata.ContentType)
		w.Header().Set("ETag", metadata.ETag)
		w.Header().Set("Last-Modified", metadata.LastModified.UTC().Format(http.TimeFormat))
		w.Header().Set("Accept-Ranges", "bytes")

		// 1. Get start, end
		var start, end int64 = 0, metadata.Size - 1
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var err error
			start, end, err = parseRange(rangeHeader, metadata.Size)
			if err != nil {
				s.sendErrorResponse(w, "InvalidRange", http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, metadata.Size))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		// 2. Calculate readSize
		readSize := end - start + 1
		w.Header().Set("Content-Length", strconv.FormatInt(readSize, 10))

		// Get object reader
		reader, err := s.GetObjectReader(bucketName, objectKey)
		if err != nil {
			log.Printf("Error getting object reader: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		// Seek to start position
		_, err = reader.Seek(start, io.SeekStart)
		if err != nil {
			log.Printf("Error seeking in file: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		// Read and send the data
		buf := make([]byte, readSize)
		n, err := io.ReadFull(reader, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Printf("Error reading file: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		_, err = w.Write(buf[:n])
		if err != nil {
			log.Printf("Error writing response: %v", err)
			return
		}

		log.Printf("Successfully streamed object %s/%s, bytes %d-%d", bucketName, objectKey, start, end)
		return

	case "PUT":
		if isDirectory {
			s.createDirectoriesAll(bucketName, objectKey)
			return
		}

		// Read the request body
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error reading request body: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		// Extract metadata from headers
		metadata := make(map[string]string)
		for key, values := range r.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
				metadata[key] = values[0]
			}
		}

		// Store the object and its metadata
		err = s.PutObject(bucketName, objectKey, body, metadata)
		if err != nil {
			log.Printf("Error storing object: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		indexFile := "." + "metadata.index"
		objectPath := filepath.Join(s.dataDir, bucketName, objectKey)
		objectMetaPath := filepath.Join(s.dataDir, bucketName, indexFile)

		s.mu.Lock()
		bucketIndex, exists := s.metadataIndex[bucketName]
		if !exists {
			bucketIndex = MetadataIndex{
				index: make(map[string][]ObjectMetadata),
				file:  objectMetaPath,
			}
			s.metadataIndex[bucketName] = bucketIndex
		}
		s.mu.Unlock()

		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		info, _ := os.Stat(objectPath)
		metadataAttr := ObjectMetadata{
			Key:              filepath.Dir(objectKey),
			FullPath:         objectPath,
			Size:             info.Size(),
			LastModified:     info.ModTime(),
			ETag:             fmt.Sprintf("%x", md5.Sum(body)), // Assuming you have the file data
			ContentType:      contentType,
			IsDirectory:      info.IsDir(),
			CustomAttributes: make(map[string]string),
		}

		// Add custom attributes from headers
		for k, v := range r.Header {
			if strings.HasPrefix(k, "X-Amz-Meta-") {
				metadataAttr.CustomAttributes[strings.TrimPrefix(k, "X-Amz-Meta-")] = v[0]
			}
		}

		s.mu.Lock()
		bucketIndex.index[objectKey] = append(bucketIndex.index[objectKey], metadataAttr)
		s.metadataIndex[bucketName] = bucketIndex
		s.mu.Unlock()

		//s.saveMetadataIndex(bucketName)

		// Send success response
		w.WriteHeader(http.StatusOK)
	case "DELETE":
		err := s.DeleteObject(bucketName, objectKey)
		if err != nil {
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func isValidBucketName(name string) bool {
	if len(name) < 3 || len(name) > 63 {
		return false
	}
	// Add more validation rules as needed
	return true
}

func getErrorMessage(code string) string {
	// Implement error messages for different error codes
	return "An error occurred"
}

func generateRequestID() string {
	// Implement request ID generation
	return "request-id"
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		//log.Printf("Received request: %s %s", r.Method, r.URL)
		next.ServeHTTP(w, r)
	})
}

type StorageBackend interface {
	CreateBucket(name string) error
	DeleteBucket(name string) error
	ListBuckets() ([]Bucket, error)
	ListObjects(bucket string) ([]Object, error)
	ListBucket(bucket string) ([]Object, error)
	GetObject(bucket, key string) ([]byte, error)
	DeleteObject(bucket, key string) error
	BucketExists(name string) bool
	BucketReplicaExists(name string) bool
	GetObjectMetadata(bucket, key string, contentType string) (*ObjectMetadata, error)
	PutObject(bucket string, obj string, data []byte, metadata map[string]string) error
	InitiateMultipartUpload(bucket, key string) (string, error)
	PutObjectPart(bucket, key, uploadID string, partNumber int, data []byte) error
	CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) error
	IsPrefix(bucket, prefix string) (bool, error)
	ListObjectsV2(bucket, prefix string, maxKeys int) ([]Object, error)
}

func (s *objStoreServer) IsPrefix(bucket, prefix string) (bool, error) {
	dirPath := filepath.Join(s.dataDir, bucket, prefix)
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return len(files) > 0, nil
}

func (s *objStoreServer) BucketExists(name string) bool {
	_, err := os.Stat(filepath.Join(s.dataDir, name))
	return !os.IsNotExist(err)
}

func (s *objStoreServer) BucketExistsReplica(name string) bool {
	_, err := os.Stat(filepath.Join(s.dataDirReplica, name))
	return !os.IsNotExist(err)
}

func (s *objStoreServer) ObjectExists(bucket, key string) bool {
	objectPath := filepath.Join(s.dataDir, bucket, key)
	_, err := os.Stat(objectPath)
	return !os.IsNotExist(err)
}

func (s *objStoreServer) ListObjects(bucket string) ([]Object, error) {
	bucketPath := filepath.Join(s.dataDir, bucket)
	var objects []Object

	files, err := ioutil.ReadDir(bucketPath)
	if err != nil {
		if os.IsNotExist(err) {
			return objects, err
		}
		return nil, err
	}

	for _, info := range files {
		relPath := info.Name()
		relPath = filepath.ToSlash(relPath) // Convert to forward slashes for S3 compatibility

		if relPath != "." {
			if info.IsDir() {
				objects = append(objects, Object{
					Key:          relPath + "/",
					LastModified: info.ModTime(),
					ETag:         "",
					Size:         0,
					IsDirectory:  true,
				})
			} else {
				objects = append(objects, Object{
					Key:          relPath,
					LastModified: info.ModTime(),
					ETag:         fmt.Sprintf("\"%x\"", md5.Sum([]byte(relPath))), // Simple ETag for demo
					Size:         info.Size(),
					IsDirectory:  false,
				})
			}
		}
	}

	return objects, nil
}

func (s *objStoreServer) ListBuckets() ([]Bucket, error) {
	files, err := ioutil.ReadDir(s.dataDir)
	if err != nil {
		return nil, err
	}

	var buckets []Bucket
	for _, file := range files {
		if file.IsDir() {
			buckets = append(buckets, Bucket{
				Name:         file.Name(),
				CreationDate: file.ModTime(),
			})
		}
	}
	return buckets, nil
}

func (s *objStoreServer) handleHeadObject(w http.ResponseWriter, r *http.Request, bucketName, objectKey string) {
	// Get object metadata
	metadata, err := s.GetObjectMetadata(bucketName, objectKey, r.Header.Get("Content-Type"))
	if err != nil {
		if os.IsNotExist(err) {
			s.sendErrorResponse(w, "NoSuchKey", http.StatusNotFound)
		} else {
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		}
		return
	}

	// Set Last-Modified header
	//lastModified := metadata.LastModified.UTC().Format(time.RFC1123)
	lastModified := metadata.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	w.Header().Set("Last-Modified", lastModified)

	// Set other required headers
	w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
	w.Header().Set("ETag", metadata.ETag)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Type", "application/octet-stream") // or the actual content type
	w.Header().Set("x-amz-request-id", generateUniqueID())     // Implement this function

	// Log the headers for debugging
	//log.Printf("Headers for %v/%v: %v", metadata, objectKey, w.Header())

	w.WriteHeader(http.StatusOK)
	//log.Printf("Successfully responded to HEAD request: bucket=%s, key=%s, size=%d, isDirectory=%v",
	//	bucketName, objectKey, metadata.Size, metadata.IsDirectory)
}

func (s *objStoreServer) handleHeadCmdBucket(w http.ResponseWriter, r *http.Request, bucketName string) error {
	path := filepath.Join(s.dataDir, bucketName)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			return fmt.Errorf("bucket not found: %s", bucketName)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			return fmt.Errorf("internal server error: %v", err)
		}
	}

	if !info.IsDir() {
		w.WriteHeader(http.StatusBadRequest)
		return fmt.Errorf("not a bucket: %s", bucketName)
	}

	// Set Last-Modified header
	lastModified := info.ModTime().UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
	w.Header().Set("Last-Modified", lastModified)

	// Set other required headers
	w.Header().Set("Content-Length", "0") // Directories typically have no content
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("x-amz-request-id", generateUniqueID())

	// Optionally set ETag for bucket
	// w.Header().Set("ETag", calculateBucketETag(bucketName))

	w.WriteHeader(http.StatusOK)
	return nil
}

func isHidden(path string) bool {
	name := filepath.Base(path)

	if strings.HasPrefix(name, ".") {
		return true
	}

	return false
}
