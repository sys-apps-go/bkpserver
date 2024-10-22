package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"
)

const (
	maxBuckets                = 32
	objectStoreDataDir        = "/mnt/objsdata/"
	objectStoreDataDirReplica = "/mnt/objsdatarepl/"
)

const (
	authTypeUnknown   = "unknown"
	authTypeSigned    = "signed"
	authTypePresigned = "presigned"
	authTypeJWT       = "jwt"
	authTypeAnonymous = "anonymous"
)

type Object struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         int64
	IsDirectory  bool
}

type ObjectMetadata struct {
	Key              string // Search key
	FullPath         string // Full path of the file
	Size             int64
	LastModified     time.Time
	ETag             string
	ContentType      string
	IsDirectory      bool
	CustomAttributes map[string]string
}

type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
	Metadata     map[string]string
}

type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}
type BucketACL struct {
	Owner       Owner        `xml:"Owner"`
	AccessRules []AccessRule `xml:"AccessControlList>Grant"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type AccessRule struct {
	Grantee    Grantee `xml:"Grantee"`
	Permission string  `xml:"Permission"`
}

type Grantee struct {
	ID          string `xml:"ID,omitempty"`
	DisplayName string `xml:"DisplayName,omitempty"`
	URI         string `xml:"URI,omitempty"`
	Type        string `xml:"type,attr"`
}

type CompleteMultipartUpload struct {
	XMLName xml.Name        `xml:"CompleteMultipartUpload"`
	Parts   []CompletedPart `xml:"Part"`
}

type CompletedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type LocationResponse struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	Location string   `xml:",chardata"`
}

type ErrorResponse struct {
	XMLName    xml.Name `xml:"Error"`
	Code       string   `xml:"Code"`
	Message    string   `xml:"Message"`
	BucketName string   `xml:"BucketName"`
	RequestID  string   `xml:"RequestId"`
	HostID     string   `xml:"HostId"`
}

type Bucket struct {
	Name         string
	CreationDate time.Time
}

type ObjectACL struct {
	Owner       Owner        `xml:"Owner"`
	AccessRules []AccessRule `xml:"AccessControlList>Grant"`
}

type MetadataIndex struct {
	index map[string][]ObjectMetadata
	file  string
}

type objStoreServer struct {
	storage            StorageBackend
	dataDir            string
	dataDirReplica     string
	fileSystemReplName string
	bucketNames        map[string]bool
	multipartUploads   sync.Map
	replicationManager *ReplicationManager
	metadataIndex      map[string]MetadataIndex
	replicationOn      bool
	mu                 sync.Mutex
}

type ReplicationManager struct {
	mu       sync.Mutex
	replicas map[string]string // map of source bucket to replica bucket
}

func NewReplicationManager() *ReplicationManager {
	return &ReplicationManager{
		replicas: make(map[string]string),
	}
}

func (rm *ReplicationManager) AddReplica(sourceBucket, replicaBucket string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.replicas[sourceBucket] = replicaBucket
}

func (rm *ReplicationManager) GetReplica(sourceBucket string) (string, bool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	replica, exists := rm.replicas[sourceBucket]
	return replica, exists
}

func createFileSystemBackend(dataDir, dataDirRepl string) error {
	err := newFileSystemBackend(dataDir)
	if err != nil {
		return err
	}
	err = newFileSystemBackend(dataDirRepl)
	if err != nil {
		return err
	}
	return nil
}

func newFileSystemBackend(dataDir string) error {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		os.MkdirAll(dataDir, 0755)
	}

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		fmt.Printf("Failed to create directory %v\n", dataDir)
		return err
	}
	return nil
}

func (s *objStoreServer) CreateBucket(name string) error {
	if s.BucketExists(name) {
		return nil
	}
	return os.Mkdir(filepath.Join(s.dataDir, name), 0755)
}

func (s *objStoreServer) CreateBucketReplica(name string) error {
	if s.BucketExistsReplica(name) {
		return nil
	}
	return os.Mkdir(filepath.Join(s.dataDirReplica, name), 0755)
}

func (s *objStoreServer) GetObject(bucket, key string) ([]byte, error) {
	path := filepath.Join(s.dataDir, bucket, key)
	return ioutil.ReadFile(path)
}

func (s *objStoreServer) DeleteObject(bucket, key string) error {
	path := filepath.Join(s.dataDir, bucket, key)
	return os.RemoveAll(path)
}

func newLiteObjectStoreServer(primaryFs, replicaFs string) *objStoreServer {
	err := createFileSystemBackend(primaryFs, replicaFs)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	s := &objStoreServer{
		dataDir:            primaryFs,
		dataDirReplica:     replicaFs,
		replicationManager: NewReplicationManager(),
		bucketNames:        make(map[string]bool),
		metadataIndex:      make(map[string]MetadataIndex),
		replicationOn:      false,
	}
	return s
}

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

		// Create replica bucket
		replicaBucket := bucketName
		err = s.CreateBucketReplica(replicaBucket)
		if err != nil {
			http.Error(w, "Failed to create replica bucket: "+err.Error(), http.StatusInternalServerError)
			return
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

		err = s.replicateObject(bucketName, objectKey, body, metadata)
		if err != nil {
			log.Printf("Failed to replicate object %s/%s: %v", bucketName, objectKey, err)
			// Optionally handle the replication failure
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

func main() {
	app := &cli.App{
		Name:  "objstore-config",
		Usage: "Retrieve ObjStore configuration",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "url",
				Aliases: []string{"u"},
				Value:   "http://localhost:50051",
				Usage:   "ObjStore server URL",
			},
			&cli.StringFlag{
				Name:    "access-key-id",
				Aliases: []string{"ak"},
				Value:   "objstoreadmin",
				Usage:   "ObjStore access key ID",
			},
			&cli.StringFlag{
				Name:    "secret-access-key",
				Aliases: []string{"sk"},
				Value:   "objstoreadmin",
				Usage:   "ObjStore secret access key",
			},
			&cli.StringFlag{
				Name:    "filesystem-drive",
				Aliases: []string{"f"},
				Value:   objectStoreDataDir,
				Usage:   "Filesystem directory for ObjStore",
			},
			&cli.StringFlag{
				Name:    "filesystem-replicate-drive",
				Aliases: []string{"r"},
				Value:   objectStoreDataDirReplica,
				Usage:   "Filesystem directory for ObjStore Replica",
			},
		},
		Action: func(c *cli.Context) error {
			urlStr := c.String("url")
			accessKeyID := c.String("access-key-id")
			secretAccessKey := c.String("secret-access-key")
			filesystemDrive := c.String("filesystem-drive")
			filesystemReplicaDrive := c.String("filesystem-replicate-drive")

			parsedURL, err := url.Parse(urlStr)
			if err != nil {
				return fmt.Errorf("invalid URL: %w", err)
			}

			// Determine the port to use
			port := parsedURL.Port()
			if port == "" {
				port = "50051" // Default port if none specified
			}

			objStoreServer := newLiteObjectStoreServer(filesystemDrive, filesystemReplicaDrive)

			r := mux.NewRouter()

			// Apply the authentication middleware to all routes
			r.Use(authMiddleware(accessKeyID, secretAccessKey))

			// Define routes

			r.HandleFunc("/", objStoreServer.handleRoot).Methods("GET")
			r.HandleFunc("/{bucket}/", objStoreServer.handleBucketCmdsLocation).
				Methods("GET").
				Queries("location", "")
			r.HandleFunc("/{bucket}", objStoreServer.handleBucketCmds).Methods("HEAD", "GET", "PUT", "DELETE")
			r.HandleFunc("/{bucket}/", objStoreServer.handleBucketCmds).Methods("HEAD", "GET", "PUT", "DELETE")

			r.HandleFunc("/{bucket}/{object:.+}", objStoreServer.handleNewMultipartUpload).
				Methods("POST").
				Queries("uploads", "")
			r.HandleFunc("/{bucket}/{object:.+}", objStoreServer.handleUploadPart).
				Methods("PUT").
				Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId}")
			r.HandleFunc("/{bucket}/{object:.+}", objStoreServer.handleCompleteMultipartUpload).
				Methods("POST").
				Queries("uploadId", "{uploadId}")
			r.HandleFunc("/{bucket}/{object:.+}", objStoreServer.handleObjectCmds).Methods("GET", "PUT", "DELETE", "HEAD")
			r.HandleFunc("/{bucket}/{object:.+}/", objStoreServer.handleObjectCmds).Methods("GET", "PUT", "DELETE", "HEAD")
			r.HandleFunc("/{bucket}/acl", objStoreServer.BucketACLHandler).Methods("GET", "PUT")

			// Add logging middleware
			r.Use(loggingMiddleware)

			port = ":" + port
			log.Println("Starting S3-compatible server on: ", port)
			log.Fatal(http.ListenAndServe(port, r))

			return nil
		},
	}

	// Run the app
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
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

func (s *objStoreServer) sendErrorResponse(w http.ResponseWriter, code string, status int) {
	w.WriteHeader(status)
	w.Header().Set("Content-Type", "application/xml")

	errorResponse := struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{
		Code:    code,
		Message: getErrorMessage(code),
	}

	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	if err := enc.Encode(errorResponse); err != nil {
		log.Printf("Error encoding error response: %v", err)
	}
}

func (s *objStoreServer) GetObjectMetadata(bucket, key string, contentType string) (*ObjectMetadata, error) {
	path := filepath.Join(s.dataDir, bucket, key)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	metadata := &ObjectMetadata{
		Size:         info.Size(),
		LastModified: info.ModTime(),
		IsDirectory:  info.IsDir(),
		ContentType:  contentType,
	}

	hash := md5.Sum([]byte(path))
	metadata.ETag = fmt.Sprintf("\"%x\"", hash)

	return metadata, nil
}

func (s *objStoreServer) PutObject(bucket, key string, data []byte, metadata map[string]string) error {
	fullPath := filepath.Join(s.dataDir, bucket, key)

	err := s.createDirectoriesAndControlFiles(s.dataDir, bucket, key)
	if err != nil {
		return err
	}

	err = s.createDirectoriesAndControlFiles(s.dataDirReplica, bucket, key)
	if err != nil {
		return err
	}
	// Write the file
	if err := ioutil.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	// Store metadata for regular files
	metadataPath := filepath.Join(filepath.Dir(fullPath), "."+filepath.Base(key)+".metadata")
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	if err := ioutil.WriteFile(metadataPath, metadataJSON, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	return nil
}

func (s *objStoreServer) createDirectoriesAndControlFiles(dataDir, bucket, objectKey string) error {
	fullPath := filepath.Join(dataDir, bucket, objectKey)
	os.MkdirAll(filepath.Dir(fullPath), 0755)

	if strings.HasSuffix(objectKey, "/") {
		objectKey = objectKey[0 : len(objectKey)-1]
	}
	base, dir := splitBaseDir(objectKey)

	objectCtrl := base
	if !strings.HasPrefix(objectCtrl, ".") {
		objectCtrl = "." + objectCtrl
	}
	if strings.HasSuffix(objectCtrl, "/") {
		objectCtrl = objectCtrl[0 : len(objectCtrl)-1]
	}
	objectCtrl = objectCtrl + ".metadata"
	controlFilePath := filepath.Join(dataDir, bucket, dir, objectCtrl)

	if _, err := os.Stat(controlFilePath); os.IsNotExist(err) {
		metadata := map[string]string{
			"CreatedAt": time.Now().UTC().Format(time.RFC3339),
			"Path":      controlFilePath,
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %v", err)
		}
		if err := ioutil.WriteFile(controlFilePath, metadataJSON, 0644); err != nil {
			return fmt.Errorf("failed to write control file: %v", err)
		}
	}
	return nil
}

func (s *objStoreServer) handleMultipartUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]

	switch r.Method {
	case "POST":
		// Check if this is an initiate multipart upload request
		if r.URL.Query().Get("uploads") != "" {
			s.initiateMultipartUpload(w, r, bucketName, objectKey)
			return
		}
		// Handle other POST requests (e.g., completing multipart upload)
		// ... (handle other methods for multipart operations)
	}
}

func (s *objStoreServer) InitiateMultipartUpload(bucket, key string) (string, error) {
	uploadID := generateUploadID()
	uploadDir := filepath.Join(s.dataDir, bucket, fmt.Sprintf("%s.%s", key, uploadID))
	err := os.MkdirAll(uploadDir, 0755)
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

func (s *objStoreServer) initiateMultipartUpload(w http.ResponseWriter, r *http.Request, bucketName, objectKey string) {
	// Generate upload ID
	uploadID, err := s.InitiateMultipartUpload(bucketName, objectKey)
	if err != nil {
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		return
	}

	// Prepare and send XML response
	response := struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadID: uploadID,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *objStoreServer) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	bucketName := vars["bucket"]
	objectKey := vars["key"]

	// Initiate the multipart upload
	uploadID, err := s.InitiateMultipartUpload(bucketName, objectKey)
	if err != nil {
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		return
	}

	// Prepare the response
	response := struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}{
		Xmlns:    "http://s3.amazonaws.com/doc/2006-03-01/",
		Bucket:   bucketName,
		Key:      objectKey,
		UploadID: uploadID,
	}

	// Send the response
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *objStoreServer) handleNewMultipartUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]
	// Generate a unique upload ID
	//uploadID := generateUploadID()
	// Store initial multipart upload info in your backend
	uploadID, err := s.InitiateMultipartUpload(bucketName, objectKey)
	if err != nil {
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		return
	}

	// Prepare XML response
	response := InitiateMultipartUploadResult{
		Bucket:   bucketName,
		Key:      objectKey,
		UploadID: uploadID,
	}

	// Send XML response
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func generateUploadID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (s *objStoreServer) handleUploadPart(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]
	uploadID := vars["uploadId"]
	partNumber, err := strconv.Atoi(vars["partNumber"])
	if err != nil {
		s.sendErrorResponse(w, "InvalidArgument", http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		return
	}
	err = s.PutObjectPart(bucketName, objectKey, uploadID, partNumber, body)
	if err != nil {
		log.Printf("Error uploading part: %v", err)
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		return
	}

	// Calculate ETag for the part
	etag := fmt.Sprintf("\"%x\"", md5.Sum(body))

	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

func (s *objStoreServer) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]
	uploadID := vars["uploadId"]
	var completeMultipartUpload CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&completeMultipartUpload); err != nil {
		s.sendErrorResponse(w, "MalformedXML", http.StatusBadRequest)
		return
	}

	err := s.CompleteMultipartUpload(bucketName, objectKey, uploadID, completeMultipartUpload.Parts)
	if err != nil {
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		return
	}

	response := CompleteMultipartUploadResult{
		Location: fmt.Sprintf("/%s/%s", bucketName, objectKey),
		Bucket:   bucketName,
		Key:      objectKey,
		ETag:     "\"" + calculateETag(completeMultipartUpload.Parts) + "\"",
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(response)
}

func (s *objStoreServer) PutObjectPart(bucket, key, uploadID string, partNumber int, data []byte) error {
	// Create the directory for this multipart upload if it doesn't exist
	uploadDir := filepath.Join(s.dataDir, bucket, fmt.Sprintf("%s.%s", key, uploadID))
	if err := os.MkdirAll(uploadDir, 0755); err != nil {
		return fmt.Errorf("failed to create upload directory: %v", err)
	}

	// Create the part file
	partPath := filepath.Join(uploadDir, fmt.Sprintf("part.%d", partNumber))
	if err := ioutil.WriteFile(partPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write part file: %v", err)
	}

	return nil
}

func (s *objStoreServer) CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) error {
	uploadDir := filepath.Join(s.dataDir, bucket, fmt.Sprintf("%s.%s", key, uploadID))
	finalPath := filepath.Join(s.dataDir, bucket, key)

	// Open the final file
	finalFile, err := os.Create(finalPath)
	if err != nil {
		return fmt.Errorf("failed to create final file: %v", err)
	}
	defer finalFile.Close()

	// Sort parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Combine all parts
	for _, part := range parts {
		partPath := filepath.Join(uploadDir, fmt.Sprintf("part.%d", part.PartNumber))
		partData, err := ioutil.ReadFile(partPath)
		if err != nil {
			return fmt.Errorf("failed to read part file: %v", err)
		}

		_, err = finalFile.Write(partData)
		if err != nil {
			return fmt.Errorf("failed to write to final file: %v", err)
		}
	}

	// Create control file
	controlFileName := "." + filepath.Base(key) + ".metadata"
	controlFilePath := filepath.Join(filepath.Dir(finalPath), controlFileName)

	metadata := map[string]string{
		"UploadID": uploadID,
		"Parts":    fmt.Sprintf("%d", len(parts)),
		// Add any other metadata you want to store
	}

	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	if err := ioutil.WriteFile(controlFilePath, metadataJSON, 0644); err != nil {
		return fmt.Errorf("failed to write control file: %v", err)
	}

	// Clean up the upload directory
	if err := os.RemoveAll(uploadDir); err != nil {
		// Log this error, but don't fail the operation
		log.Printf("Warning: failed to remove upload directory: %v", err)
	}

	// Replica the object to the mirror bucket
	err = s.replicateObjectFileCopy(bucket, key)
	if err != nil {
		log.Printf("Failed to replicate object %s/%s: %v", bucket, key, err)
		// Optionally, you could return this error if you want to ensure replication succeeds
		// return fmt.Errorf("failed to replicate object: %v", err)
	}

	return nil
}

func (s *objStoreServer) replicateObject(bucketName, key string, data []byte, metadata map[string]string) error {
	replicaPath := filepath.Join(s.dataDirReplica, bucketName, key)

	// Ensure the directory structure exists
	dir := filepath.Dir(replicaPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory structure: %v", err)
	}

	// Create the file
	replicaFile, err := os.Create(replicaPath)
	if err != nil {
		return fmt.Errorf("failed to create replica file: %v", err)
	}
	defer replicaFile.Close()

	// Write the data to the file
	_, err = replicaFile.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write file contents: %v", err)
	}
	s.createControlFile(s.dataDirReplica, bucketName, key)

	return nil
}

func (s *objStoreServer) replicateObjectFileCopy(bucketName, key string) error {
	sourcePath := filepath.Join(s.dataDir, bucketName, key)
	replicaPath := filepath.Join(s.dataDirReplica, bucketName, key)

	// Ensure the directory structure exists
	dir := filepath.Dir(replicaPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory structure: %v", err)
	}

	// Open the source file
	sourceFile, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %v", err)
	}
	defer sourceFile.Close()

	// Create the replica file
	replicaFile, err := os.Create(replicaPath)
	if err != nil {
		return fmt.Errorf("failed to create replica file: %v", err)
	}
	defer replicaFile.Close()

	// Copy the contents
	_, err = io.Copy(replicaFile, sourceFile)
	if err != nil {
		return fmt.Errorf("failed to copy file contents: %v", err)
	}

	return nil
}

func (s *objStoreServer) copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	_, err = io.Copy(destFile, sourceFile)
	if err != nil {
		return err
	}

	return nil
}

func calculateETag(parts []CompletedPart) string {
	if len(parts) == 0 {
		return ""
	}

	// Sort parts by part number
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})

	// Concatenate all ETags
	var concatenatedETags []byte
	for _, part := range parts {
		etagBytes, _ := hex.DecodeString(strings.Trim(part.ETag, "\""))
		concatenatedETags = append(concatenatedETags, etagBytes...)
	}

	// Calculate MD5 of concatenated ETags
	finalMD5 := md5.Sum(concatenatedETags)
	finalETag := hex.EncodeToString(finalMD5[:])

	// Return ETag in the format "md5sum-numberOfParts"
	return fmt.Sprintf("%s-%d", finalETag, len(parts))
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

func (s *objStoreServer) ListObjectsV2(bucket, prefix string, maxKeys int) ([]Object, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	bucketPath := filepath.Join(s.dataDir, bucket)
	prefixPath := filepath.Join(bucketPath, prefix)

	var objects []Object
	var count int

	err := filepath.Walk(prefixPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}

		relPath, err := filepath.Rel(bucketPath, path)
		if err != nil {
			return err
		}

		if !info.IsDir() {
			// Check for .metadata file
			metadataPath := filepath.Join(filepath.Dir(path), "."+info.Name()+".metadata")
			if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
				// Skip if .metadata file doesn't exist
				return nil
			}
		}

		relPath = filepath.ToSlash(relPath)
		if relPath != "." {
			if info.IsDir() {
				objects = append(objects, Object{
					Key:          relPath + "/",
					LastModified: info.ModTime(),
					ETag:         "",
					Size:         0,
					IsDirectory:  true,
				})
				count++
			} else {
				objects = append(objects, Object{
					Key:          relPath,
					LastModified: info.ModTime(),
					ETag:         fmt.Sprintf("\"%x\"", md5.Sum([]byte(relPath))), // Simple ETag for demo
					Size:         info.Size(),
					IsDirectory:  false,
				})
				count++
			}
		}

		if maxKeys != 0 && count >= maxKeys {
		//	return filepath.SkipDir
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return objects, nil
}

func (s *objStoreServer) ListBucket(bucket string) ([]Object, error) {
	bucketPath := filepath.Join(s.dataDir, bucket)

	entries, err := ioutil.ReadDir(bucketPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Return empty list if bucket doesn't exist
		}
		return nil, err
	}

	var objects []Object

	for _, entry := range entries {
		if isHidden(entry.Name()) {
			continue
		}

		// Check for .metadata file
		metadataPath := filepath.Join(bucketPath, "."+entry.Name()+".metadata")
		if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
			// Skip if .metadata file doesn't exist
			continue
		}

		relPath := filepath.ToSlash(entry.Name())
		key := relPath
		if entry.IsDir() {
			key += "/"
		}

		objects = append(objects, Object{
			Key:          key,
			LastModified: entry.ModTime(),
			ETag:         fmt.Sprintf("\"%x\"", md5.Sum([]byte(key))),
			Size:         entry.Size(),
			IsDirectory:  entry.IsDir(),
		})
	}

	return objects, nil
}

func authMiddlewareNew(accessKeyID, secretAccessKey string) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract authentication information
			authInfo := extractAuthInfo(r)

			// Validate the authentication
			if !validateAuth(authInfo, accessKeyID, secretAccessKey) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			// Determine the authentication type
			authType := getRequestAuthType(r)

			// Handle the request based on the authentication type
			switch authType {
			case authTypeSigned:
				fmt.Fprintf(w, "Request is authenticated using signed method.")
			case authTypePresigned:
				fmt.Fprintf(w, "Request is authenticated using presigned URL.")
			case authTypeJWT:
				fmt.Fprintf(w, "Request is authenticated using JWT.")
			case authTypeAnonymous:
				fmt.Fprintf(w, "Request is anonymous.")
			default:
				http.Error(w, "Unknown authentication type", http.StatusUnauthorized)
				return
			}

			// If authentication is successful and the request is handled, proceed to the next handler
			next.ServeHTTP(w, r)
		})
	}
}

// Function to determine authentication type
func getRequestAuthType(r *http.Request) string {
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		if isRequestSignatureV4(r) {
			return authTypeSigned
		} else if isRequestPresignedSignature(r) {
			return authTypePresigned
		} else if isRequestJWT(r) {
			return authTypeJWT
		}
	}

	if r.FormValue("action") != "" {
		return authTypeAnonymous
	}

	return authTypeAnonymous
}

// Implement signature check logic
func isRequestSignatureV4(r *http.Request) bool {
	authHeader := r.Header.Get("Authorization")
	return strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256")
}

// Implement presigned URL check logic
func isRequestPresignedSignature(r *http.Request) bool {
	// Check for typical presigned URL parameters
	return r.URL.Query().Get("X-Amz-Signature") != "" &&
		r.URL.Query().Get("X-Amz-Date") != "" &&
		r.URL.Query().Get("X-Amz-Expires") != ""
}

// Implement JWT validation logic
func isRequestJWT(r *http.Request) bool {
	authHeader := r.Header.Get("Authorization")
	return strings.HasPrefix(authHeader, "Bearer ")
}

func authMiddleware(accessKeyID, secretAccessKey string) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract authentication information
			authInfo := extractAuthInfo(r)

			// Validate the authentication
			if !validateAuth(authInfo, accessKeyID, secretAccessKey) {
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
				return
			}

			// If authentication is successful, proceed to the next handler
			next.ServeHTTP(w, r)
		})
	}
}

func extractAuthInfo(r *http.Request) map[string]string {
	authInfo := make(map[string]string)

	// Check Authorization header
	if auth := r.Header.Get("Authorization"); auth != "" {
		// Parse Authorization header
		authInfo["type"] = "header"
		authInfo["auth"] = auth
	} else {
		// Check query parameters
		authInfo["type"] = "query"
		authInfo["accessKey"] = r.URL.Query().Get("AWSAccessKeyId")
		authInfo["signature"] = r.URL.Query().Get("Signature")
	}

	return authInfo
}

func validateAuth(authInfo map[string]string, accessKeyID, secretAccessKey string) bool {
	if authInfo["type"] == "header" {
		// Verify header-based auth
		return strings.Contains(authInfo["auth"], accessKeyID)
	} else if authInfo["type"] == "query" {
		// Verify query-based auth
		return authInfo["accessKey"] == accessKeyID && authInfo["signature"] != ""
	}

	return false
}

func generateUniqueID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return ""
	}
	return hex.EncodeToString(b)
}

func formatMinioTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// Helper function to extract metadata from headers
func extractMetadata(header http.Header) map[string]string {
	metadata := make(map[string]string)
	for key, values := range header {
		if strings.HasPrefix(strings.ToLower(key), "x-amz-meta-") {
			metadata[key] = values[0]
		}
	}
	return metadata
}

func (rm *ReplicationManager) RemoveReplica(sourceBucket string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.replicas, sourceBucket)
}

func (s *objStoreServer) deleteBucket(bucket string) error {
	bucketPath := filepath.Join(s.dataDir, bucket)
	err := s.DeleteBucket(bucketPath)
	if err == nil {
		bucketPath = filepath.Join(s.dataDirReplica, bucket)
		err = s.DeleteBucket(bucketPath)
	}
	return err
}

func (s *objStoreServer) DeleteBucket(bucketPath string) error {
	// Check if the bucket exists
	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		return nil
	}

	// Remove the bucket directory and all its contents
	err := os.RemoveAll(bucketPath)
	if err != nil {
		return fmt.Errorf("failed to delete bucket %s: %v", bucketPath, err)
	}

	if _, err := os.Stat(bucketPath); os.IsNotExist(err) {
		return fmt.Errorf("bucket %s does not exist", bucketPath)
	}

	return nil
}

func (s *objStoreServer) handleBucketCmdsLocation(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Check if the bucket exists
	if !s.BucketExists(bucket) {
		s.sendErrorResponse(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	// Get the bucket location
	location, _ := s.getBucketLocation(bucket)

	// Create the response
	response := LocationResponse{
		Location: location,
	}

	// Marshal the response to XML
	xmlResponse, err := xml.MarshalIndent(response, "", "  ")
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Set headers and write response
	w.Header().Set("Content-Type", "application/xml")
	w.Write([]byte(xml.Header + string(xmlResponse)))
}

func (s *objStoreServer) getBucketLocation(bucket string) (string, error) {
	return "us-east-1", nil
}

func (s *objStoreServer) createDirectoriesAll(bucket, objectKey string) error {
	fullPath := filepath.Join(s.dataDir, bucket, objectKey)
	err := os.MkdirAll(fullPath, 0755)
	if err != nil {
		return err
	}

	fullPath = filepath.Join(s.dataDirReplica, bucket, objectKey)
	err = os.MkdirAll(fullPath, 0755)
	if err != nil {
		return err
	}
	return nil
}

func (s *objStoreServer) createControlFile(dataDir, bucket, objectKey string) error {
	fullPath := filepath.Join(dataDir, bucket, objectKey)

	if strings.HasSuffix(objectKey, "/") {
		return nil
	}

	base, dir := splitBaseDir(objectKey)

	objectCtrl := base
	if !strings.HasPrefix(objectCtrl, ".") {
		objectCtrl = "." + objectCtrl
	}
	if strings.HasSuffix(objectCtrl, "/") {
		objectCtrl = objectCtrl[0 : len(objectCtrl)-1]
	}
	objectCtrl = objectCtrl + ".metadata"
	fullPath = filepath.Join(dataDir, bucket, dir, objectCtrl)

	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		metadata := map[string]string{
			"CreatedAt": time.Now().UTC().Format(time.RFC3339),
			"Path":      fullPath,
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %v", err)
		}
		if err := ioutil.WriteFile(fullPath, metadataJSON, 0644); err != nil {
			return fmt.Errorf("failed to write control file: %v", err)
		}
	}
	return nil
}

func splitBaseDir(path string) (string, string) {
	lastIndex := strings.LastIndex(path, "/")
	if lastIndex != -1 {
		return path[lastIndex+1:], path[:lastIndex]
	}

	return path, ""
}

func (s *objStoreServer) PutBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// 1. Check if the bucket exists
	s.mu.Lock()
	exists := s.bucketNames[bucket]
	s.mu.Unlock()

	if !exists {
		s.writeErrorResponse(ctx, w, APIError{
			Code:           "NoSuchBucket",
			Description:    "The specified bucket does not exist",
			HTTPStatusCode: http.StatusNotFound,
		}, r.URL)
		return
	}

	// 2. Check if the user has permission to set the ACL
	// Note: You'll need to implement your own permission checking mechanism
	if !s.isAllowed(ctx, r, bucket, "s3:PutBucketAcl") {
		s.writeErrorResponse(ctx, w, APIError{
			Code:           "AccessDenied",
			Description:    "You don't have permission to set the ACL on this bucket",
			HTTPStatusCode: http.StatusForbidden,
		}, r.URL)
		return
	}

	// 3. Parse the ACL from the request
	var acl *BucketACL
	var err error

	aclHeader := r.Header.Get("x-amz-acl")
	if aclHeader != "" {
		acl, err = s.parseBucketACLHeader(aclHeader)
		if err != nil {
			log.Printf("Error parsing ACL header: %v", err)
			s.writeErrorResponse(ctx, w, APIError{
				Code:           "InvalidArgument",
				Description:    "Invalid ACL header",
				HTTPStatusCode: http.StatusBadRequest,
			}, r.URL)
			return
		}
	} else {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error reading request body: %v", err)
			s.writeErrorResponse(ctx, w, APIError{
				Code:           "InvalidArgument",
				Description:    "Unable to read request body",
				HTTPStatusCode: http.StatusBadRequest,
			}, r.URL)
			return
		}
		acl, err = s.parseBucketACLFromBody(bodyBytes)
		if err != nil {
			log.Printf("Error parsing ACL from body: %v", err)
			s.writeErrorResponse(ctx, w, APIError{
				Code:           "MalformedACLError",
				Description:    "The XML you provided was not well-formed or did not validate against our published schema",
				HTTPStatusCode: http.StatusBadRequest,
			}, r.URL)
			return
		}
	}

	// 4. Validate the ACL
	if err := s.validateBucketACL(acl); err != nil {
		log.Printf("ACL validation failed: %v", err)
		s.writeErrorResponse(ctx, w, APIError{
			Code:           "InvalidArgument",
			Description:    "Invalid ACL structure",
			HTTPStatusCode: http.StatusBadRequest,
		}, r.URL)
		return
	}

	// 5. Apply the ACL to the bucket
	// Note: You'll need to implement this method to work with your storage backend
	if err := s.setBucketACL(ctx, bucket, acl); err != nil {
		log.Printf("Error setting bucket ACL: %v", err)
		s.writeErrorResponse(ctx, w, APIError{
			Code:           "InternalError",
			Description:    "We encountered an internal error. Please try again.",
			HTTPStatusCode: http.StatusInternalServerError,
		}, r.URL)
		return
	}

	// 6. Send a success response
	s.writeSuccessResponseHeadersOnly(w)
	log.Printf("Successfully set ACL for bucket: %s", bucket)
}

// Helper methods that you'll need to implement:

func (s *objStoreServer) isAllowed(ctx context.Context, r *http.Request, bucket, action string) bool {
	// Get the user from the request context
	// This assumes you have middleware that authenticates the user and puts their info in the context
	user, ok := ctx.Value("user").(string)
	if !ok {
		// If there's no user in the context, deny access
		return false
	}

	// Check if the user is objstoreadmin
	if user == "objstoreadmin" {
		// objstoreadmin has full permissions
		return true
	}

	// For other users, you might want to implement more granular permission checks
	// This could involve checking against a database of user roles and permissions
	// For now, we'll just deny access to non-admin users
	return false
}

func (s *objStoreServer) parseBucketACLHeader(header string) (*BucketACL, error) {
	// Implement parsing logic for predefined ACLs
	switch strings.ToLower(header) {
	case "private":
		return &BucketACL{
			Owner: Owner{ID: "OwnerID", DisplayName: "OwnerName"},
			AccessRules: []AccessRule{
				{
					Grantee:    Grantee{ID: "OwnerID", DisplayName: "OwnerName", Type: "CanonicalUser"},
					Permission: "FULL_CONTROL",
				},
			},
		}, nil
	case "public-read":
		return &BucketACL{
			Owner: Owner{ID: "OwnerID", DisplayName: "OwnerName"},
			AccessRules: []AccessRule{
				{
					Grantee:    Grantee{ID: "OwnerID", DisplayName: "OwnerName", Type: "CanonicalUser"},
					Permission: "FULL_CONTROL",
				},
				{
					Grantee:    Grantee{URI: "http://acs.amazonaws.com/groups/global/AllUsers", Type: "Group"},
					Permission: "READ",
				},
			},
		}, nil
	case "public-read-write":
		return &BucketACL{
			Owner: Owner{ID: "OwnerID", DisplayName: "OwnerName"},
			AccessRules: []AccessRule{
				{
					Grantee:    Grantee{ID: "OwnerID", DisplayName: "OwnerName", Type: "CanonicalUser"},
					Permission: "FULL_CONTROL",
				},
				{
					Grantee:    Grantee{URI: "http://acs.amazonaws.com/groups/global/AllUsers", Type: "Group"},
					Permission: "READ",
				},
				{
					Grantee:    Grantee{URI: "http://acs.amazonaws.com/groups/global/AllUsers", Type: "Group"},
					Permission: "WRITE",
				},
			},
		}, nil
	// Add other predefined ACLs as needed
	default:
		return nil, errors.New("invalid ACL header")
	}
}

func (s *objStoreServer) parseBucketACLFromBody(body []byte) (*BucketACL, error) {
	// Implement parsing logic for XML ACL in the request body
	var acl BucketACL
	err := xml.Unmarshal(body, &acl)
	if err != nil {
		return nil, err
	}

	// Validate the parsed ACL
	if acl.Owner.ID == "" || len(acl.AccessRules) == 0 {
		return nil, errors.New("invalid ACL structure")
	}

	return &acl, nil
}

func (s *objStoreServer) validateBucketACL(acl *BucketACL) error {
	if acl == nil {
		return errors.New("ACL is nil")
	}

	if acl.Owner.ID == "" {
		return errors.New("ACL must have an owner")
	}

	if len(acl.AccessRules) == 0 {
		return errors.New("ACL must have at least one access rule")
	}

	for _, rule := range acl.AccessRules {
		if rule.Grantee.Type == "" {
			return errors.New("each grantee must have a type")
		}

		switch rule.Grantee.Type {
		case "CanonicalUser":
			if rule.Grantee.ID == "" {
				return errors.New("CanonicalUser grantee must have an ID")
			}
		case "Group":
			if rule.Grantee.URI == "" {
				return errors.New("Group grantee must have a URI")
			}
		default:
			return fmt.Errorf("unknown grantee type: %s", rule.Grantee.Type)
		}

		switch rule.Permission {
		case "READ", "WRITE", "READ_ACP", "WRITE_ACP", "FULL_CONTROL":
			// Valid permissions
		default:
			return fmt.Errorf("invalid permission: %s", rule.Permission)
		}
	}

	return nil
}

func (s *objStoreServer) setBucketACL(ctx context.Context, bucket string, acl *BucketACL) error {
	// First, validate the ACL
	if err := s.validateBucketACL(acl); err != nil {
		return err
	}

	// Here, you would typically update the ACL in your storage backend
	// This is a placeholder implementation - you'll need to replace this
	// with actual logic to update your storage system
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the bucket exists
	if _, exists := s.bucketNames[bucket]; !exists {
		return errors.New("bucket does not exist")
	}

	// In a real implementation, you might do something like:
	// s.storage.UpdateBucketACL(bucket, acl)

	// For now, we'll just log that we're setting the ACL
	log.Printf("Setting ACL for bucket %s: %+v", bucket, acl)

	return nil
}

func (s *objStoreServer) writeErrorResponse(ctx context.Context, w http.ResponseWriter, apiErr APIError, reqURL *url.URL) {
	w.WriteHeader(apiErr.HTTPStatusCode)

	errorResponse := ErrorResponse{
		Code:       apiErr.Code,
		Message:    apiErr.Description,
		BucketName: reqURL.Query().Get("bucket"),
		RequestID:  ctx.Value("requestID").(string),
		HostID:     "YourHostID", // Replace with actual host ID if available
	}

	encoder := xml.NewEncoder(w)
	encoder.Encode(errorResponse)
}

func (s *objStoreServer) writeSuccessResponseHeadersOnly(w http.ResponseWriter) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("x-amz-request-id", uuid.New().String())
	// Add any other headers you want to include in a successful response
}

func (s *objStoreServer) GetBucketACLHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	// 1. Check if the bucket exists
	if !s.BucketExists(bucketName) {
		s.writeErrorResponse(ctx, w, APIError{
			Code:           "NoSuchBucket",
			Description:    "The specified bucket does not exist",
			HTTPStatusCode: http.StatusNotFound,
		}, r.URL)
		return
	}

	// 2. Check if the user has permission to get the ACL
	if !s.isAllowed(ctx, r, bucketName, "s3:GetBucketAcl") {
		s.writeErrorResponse(ctx, w, APIError{
			Code:           "AccessDenied",
			Description:    "You don't have permission to access the ACL for this bucket",
			HTTPStatusCode: http.StatusForbidden,
		}, r.URL)
		return
	}

	// 3. Retrieve the ACL for the bucket
	acl, err := s.getBucketACL(ctx, bucketName)
	if err != nil {
		s.writeErrorResponse(ctx, w, APIError{
			Code:           "InternalError",
			Description:    "We encountered an internal error. Please try again.",
			HTTPStatusCode: http.StatusInternalServerError,
		}, r.URL)
		return
	}

	// 4. Prepare the response
	response := &GetBucketACLResponse{
		Owner:       acl.Owner,
		AccessRules: acl.AccessRules,
	}

	// 5. Write the response
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	encoder := xml.NewEncoder(w)
	if err := encoder.Encode(response); err != nil {
		log.Printf("Error encoding ACL response: %v", err)
		// At this point, we've already started writing the response,
		// so we can't send an error response. We'll just have to log it.
	}
}

// GetBucketACLResponse represents the XML structure for the GetBucketACL response
type GetBucketACLResponse struct {
	XMLName     xml.Name     `xml:"AccessControlPolicy"`
	Owner       Owner        `xml:"Owner"`
	AccessRules []AccessRule `xml:"AccessControlList>Grant"`
}

// You'll need to implement this method to retrieve the ACL from your storage backend
func (s *objStoreServer) getBucketACL(ctx context.Context, bucketName string) (*BucketACL, error) {
	// This is a placeholder implementation
	// In a real scenario, you would retrieve this from your storage backend
	return &BucketACL{
		Owner: Owner{
			ID:          "OwnerID",
			DisplayName: "OwnerName",
		},
		AccessRules: []AccessRule{
			{
				Grantee: Grantee{
					ID:          "OwnerID",
					DisplayName: "OwnerName",
					Type:        "CanonicalUser",
				},
				Permission: "FULL_CONTROL",
			},
		},
	}, nil
}

func (s *objStoreServer) BucketACLHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Check if the bucket exists
	if !s.BucketExists(bucket) {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "NoSuchBucket",
			Description:    "The specified bucket does not exist",
			HTTPStatusCode: http.StatusNotFound,
		}, r.URL)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.GetBucketACLHandler(w, r)
	case http.MethodPut:
		s.PutBucketACLHandler(w, r)
	default:
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "MethodNotAllowed",
			Description:    "The specified method is not allowed against this resource",
			HTTPStatusCode: http.StatusMethodNotAllowed,
		}, r.URL)
	}
}

func (s *objStoreServer) PutObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	versionID := r.URL.Query().Get("versionId")

	// 1. Check if bucket and object exist
	if !s.BucketExists(bucket) || !s.ObjectExists(bucket, key) {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "NoSuchKey",
			Description:    "The specified key does not exist.",
			HTTPStatusCode: http.StatusNotFound,
		}, r.URL)
		return
	}

	// 2. Check permissions
	if !s.isAllowed(r.Context(), r, bucket, "s3:PutObjectAcl") {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "AccessDenied",
			Description:    "Access Denied",
			HTTPStatusCode: http.StatusForbidden,
		}, r.URL)
		return
	}

	// 3. Parse ACL
	acl, err := s.parseObjectACLFromRequest(r)
	if err != nil {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "MalformedACLError",
			Description:    "The XML you provided was not well-formed or did not validate against our published schema",
			HTTPStatusCode: http.StatusBadRequest,
		}, r.URL)
		return
	}

	// 4. Apply ACL
	err = s.setObjectACL(bucket, key, versionID, acl)
	if err != nil {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "InternalError",
			Description:    "We encountered an internal error. Please try again.",
			HTTPStatusCode: http.StatusInternalServerError,
		}, r.URL)
		return
	}

	// 5. Send success response
	w.WriteHeader(http.StatusOK)
}

func (s *objStoreServer) GetObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	versionID := r.URL.Query().Get("versionId")

	// 1. Check if bucket and object exist
	if !s.BucketExists(bucket) || !s.ObjectExists(bucket, key) {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "NoSuchKey",
			Description:    "The specified key does not exist.",
			HTTPStatusCode: http.StatusNotFound,
		}, r.URL)
		return
	}

	// 2. Check permissions
	if !s.isAllowed(r.Context(), r, bucket, "s3:GetObjectAcl") {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "AccessDenied",
			Description:    "Access Denied",
			HTTPStatusCode: http.StatusForbidden,
		}, r.URL)
		return
	}

	// 3. Get the ACL
	acl, err := s.getObjectACL(bucket, key, versionID)
	if err != nil {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "InternalError",
			Description:    "We encountered an internal error. Please try again.",
			HTTPStatusCode: http.StatusInternalServerError,
		}, r.URL)
		return
	}

	// 4. Prepare and send the response
	response := &GetObjectACLResponse{
		Owner:       acl.Owner,
		AccessRules: acl.AccessRules,
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	if err := xml.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding ACL response: %v", err)
	}
}

type GetObjectACLResponse struct {
	XMLName     xml.Name     `xml:"AccessControlPolicy"`
	Owner       Owner        `xml:"Owner"`
	AccessRules []AccessRule `xml:"AccessControlList>Grant"`
}

func (s *objStoreServer) getObjectACL(bucket, key, versionID string) (*ObjectACL, error) {
	// Implementation to get the ACL from your storage backend
	// This is a placeholder - you need to implement this based on your storage system
	return &ObjectACL{
		Owner: Owner{ID: "OwnerID", DisplayName: "OwnerName"},
		AccessRules: []AccessRule{
			{
				Grantee:    Grantee{ID: "OwnerID", DisplayName: "OwnerName", Type: "CanonicalUser"},
				Permission: "FULL_CONTROL",
			},
		},
	}, nil
}

func (s *objStoreServer) parseObjectACLFromRequest(r *http.Request) (*ObjectACL, error) {
	// Check for canned ACL in header
	cannedACL := r.Header.Get("x-amz-acl")
	if cannedACL != "" {
		return s.parseCannedACL(cannedACL)
	}

	// Check for grant headers
	acl := &ObjectACL{
		Owner:       Owner{ID: "OwnerID", DisplayName: "OwnerName"}, // You should set this to the actual owner
		AccessRules: []AccessRule{},
	}

	grantRead := r.Header.Get("x-amz-grant-read")
	grantWrite := r.Header.Get("x-amz-grant-write")
	grantReadACP := r.Header.Get("x-amz-grant-read-acp")
	grantWriteACP := r.Header.Get("x-amz-grant-write-acp")
	grantFullControl := r.Header.Get("x-amz-grant-full-control")

	if grantRead != "" {
		acl.AccessRules = append(acl.AccessRules, s.parseGrantHeader(grantRead, "READ")...)
	}
	if grantWrite != "" {
		acl.AccessRules = append(acl.AccessRules, s.parseGrantHeader(grantWrite, "WRITE")...)
	}
	if grantReadACP != "" {
		acl.AccessRules = append(acl.AccessRules, s.parseGrantHeader(grantReadACP, "READ_ACP")...)
	}
	if grantWriteACP != "" {
		acl.AccessRules = append(acl.AccessRules, s.parseGrantHeader(grantWriteACP, "WRITE_ACP")...)
	}
	if grantFullControl != "" {
		acl.AccessRules = append(acl.AccessRules, s.parseGrantHeader(grantFullControl, "FULL_CONTROL")...)
	}

	// If no headers, try to parse from body
	if len(acl.AccessRules) == 0 {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, err
		}
		return s.parseACLFromXML(body)
	}

	return acl, nil
}

func (s *objStoreServer) parseCannedACL(cannedACL string) (*ObjectACL, error) {
	// Implement parsing for canned ACLs (private, public-read, etc.)
	// This is a simplified example
	switch cannedACL {
	case "private":
		return &ObjectACL{
			Owner: Owner{ID: "OwnerID", DisplayName: "OwnerName"},
			AccessRules: []AccessRule{
				{
					Grantee:    Grantee{ID: "OwnerID", DisplayName: "OwnerName", Type: "CanonicalUser"},
					Permission: "FULL_CONTROL",
				},
			},
		}, nil
	// Add other canned ACLs as needed
	default:
		return nil, fmt.Errorf("unsupported canned ACL: %s", cannedACL)
	}
}

func (s *objStoreServer) parseGrantHeader(grantHeader, permission string) []AccessRule {
	// Implement parsing of grant headers
	// This is a simplified example
	grantees := strings.Split(grantHeader, ",")
	rules := []AccessRule{}
	for _, grantee := range grantees {
		rules = append(rules, AccessRule{
			Grantee:    Grantee{ID: grantee, Type: "CanonicalUser"},
			Permission: permission,
		})
	}
	return rules
}

func (s *objStoreServer) parseACLFromXML(xmlBody []byte) (*ObjectACL, error) {
	var acl ObjectACL
	if err := xml.Unmarshal(xmlBody, &acl); err != nil {
		return nil, err
	}
	return &acl, nil
}

func (s *objStoreServer) setObjectACL(bucket, key, versionID string, acl *ObjectACL) error {
	// Implementation to set the ACL on the object
	// This is where you would interact with your storage backend to update the ACL
	// For example:

	// 1. Validate the ACL
	if err := s.validateObjectACL(acl); err != nil {
		return err
	}

	// 2. Update the ACL in your storage system
	// This is a placeholder - you need to implement this based on your storage system
	log.Printf("Setting ACL for object %s/%s (version: %s): %+v", bucket, key, versionID, acl)

	// 3. If you're supporting versioning, you might need to handle the versionID
	if versionID != "" {
		// Set ACL for specific version
	} else {
		// Set ACL for latest version
	}

	return nil
}

func (s *objStoreServer) validateObjectACL(acl *ObjectACL) error {
	if acl == nil {
		return errors.New("ACL is nil")
	}
	if acl.Owner.ID == "" {
		return errors.New("ACL must have an owner")
	}
	if len(acl.AccessRules) == 0 {
		return errors.New("ACL must have at least one access rule")
	}
	// Add more validation as needed
	return nil
}

func (s *objStoreServer) ObjectACLHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]
	key := vars["key"]
	versionID := r.URL.Query().Get("versionId")

	// Check if the bucket and object exist
	if !s.BucketExists(bucket) || !s.ObjectExists(bucket, key) {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "NoSuchKey",
			Description:    "The specified key does not exist.",
			HTTPStatusCode: http.StatusNotFound,
		}, r.URL)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.GetObjectACL(w, r, bucket, key, versionID)
	case http.MethodPut:
		s.PutObjectACL(w, r, bucket, key, versionID)
	default:
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "MethodNotAllowed",
			Description:    "The specified method is not allowed against this resource.",
			HTTPStatusCode: http.StatusMethodNotAllowed,
		}, r.URL)
	}
}

func (s *objStoreServer) GetObjectACL(w http.ResponseWriter, r *http.Request, bucket, key, versionID string) {
	// Check permissions
	if !s.isAllowed(r.Context(), r, bucket, "s3:GetObjectAcl") {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "AccessDenied",
			Description:    "Access Denied",
			HTTPStatusCode: http.StatusForbidden,
		}, r.URL)
		return
	}

	acl, err := s.getObjectACL(bucket, key, versionID)
	if err != nil {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "InternalError",
			Description:    "We encountered an internal error. Please try again.",
			HTTPStatusCode: http.StatusInternalServerError,
		}, r.URL)
		return
	}

	// Write the ACL response
	w.Header().Set("Content-Type", "application/xml")
	if err := xml.NewEncoder(w).Encode(acl); err != nil {
		log.Printf("Error encoding ACL response: %v", err)
	}
}

func (s *objStoreServer) PutObjectACL(w http.ResponseWriter, r *http.Request, bucket, key, versionID string) {
	// Check permissions
	if !s.isAllowed(r.Context(), r, bucket, "s3:PutObjectAcl") {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "AccessDenied",
			Description:    "Access Denied",
			HTTPStatusCode: http.StatusForbidden,
		}, r.URL)
		return
	}

	acl, err := s.parseObjectACLFromRequest(r)
	if err != nil {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "MalformedACLError",
			Description:    "The XML you provided was not well-formed or did not validate against our published schema",
			HTTPStatusCode: http.StatusBadRequest,
		}, r.URL)
		return
	}

	if err := s.setObjectACL(bucket, key, versionID, acl); err != nil {
		s.writeErrorResponse(r.Context(), w, APIError{
			Code:           "InternalError",
			Description:    "We encountered an internal error. Please try again.",
			HTTPStatusCode: http.StatusInternalServerError,
		}, r.URL)
		return
	}

	// Send success response
	w.WriteHeader(http.StatusOK)
}

func (s *objStoreServer) saveMetadataIndex(bucketName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	bucketIndex, exists := s.metadataIndex[bucketName]
	if !exists {
		return fmt.Errorf("metadata index not found for bucket: %s", bucketName)
	}

	data, err := json.Marshal(bucketIndex.index)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(bucketIndex.file, data, 0644)
}

func parseRange(rangeHeader string, size int64) (int64, int64, error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range header format")
	}
	parts := strings.Split(strings.TrimPrefix(rangeHeader, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range header format")
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start range")
	}
	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		end = size - 1
	}
	if start > end || start < 0 || end >= size {
		return 0, 0, fmt.Errorf("invalid range")
	}
	return start, end, nil
}

func (s *objStoreServer) GetObjectReader(bucketName, objectKey string) (io.ReadSeekCloser, error) {
	filePath := filepath.Join(s.dataDir, bucketName, objectKey)
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("object not found")
		}
		return nil, err
	}
	return file, nil
}
