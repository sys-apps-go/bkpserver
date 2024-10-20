package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
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

type Object struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         int64
	IsDirectory  bool
}

type ObjectMetadata struct {
	Size         int64
	LastModified time.Time
	ETag         string
	IsDirectory  bool
	ContentType  string
}

type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
	Metadata     map[string]string
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

type Bucket struct {
	Name         string
	CreationDate time.Time
}

type ltosServer struct {
	storage            StorageBackend
	dataDir            string
	dataDirReplica     string
	fileSystemReplName string
	bucketNames        map[string]bool
	multipartUploads   sync.Map
	replicationManager *ReplicationManager
	controlFileMap     map[string]bool
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

func (s *ltosServer) CreateBucket(name string) error {
	if s.BucketExists(name) {
		return nil
	}
	return os.Mkdir(filepath.Join(s.dataDir, name), 0755)
}

func (s *ltosServer) CreateBucketReplica(name string) error {
	if s.BucketExistsReplica(name) {
		return nil
	}
	return os.Mkdir(filepath.Join(s.dataDirReplica, name), 0755)
}

func (s *ltosServer) GetObject(bucket, key string) ([]byte, error) {
	path := filepath.Join(s.dataDir, bucket, key)
	return ioutil.ReadFile(path)
}

func (s *ltosServer) DeleteObject(bucket, key string) error {
	path := filepath.Join(s.dataDir, bucket, key)
	return os.RemoveAll(path)
}

func newLiteObjectStoreServer(primaryFs, replicaFs string) *ltosServer {
	err := createFileSystemBackend(primaryFs, replicaFs)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	s := &ltosServer{
		dataDir:            primaryFs,
		dataDirReplica:     replicaFs,
		replicationManager: NewReplicationManager(),
		controlFileMap:     make(map[string]bool),
		bucketNames:        make(map[string]bool),
	}
	return s
}

func (s *ltosServer) handleRoot(w http.ResponseWriter, r *http.Request) {
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
			DisplayName: "ltosServerOwner",
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

func (s *ltosServer) handleBucketCmds(w http.ResponseWriter, r *http.Request) {
	var objects []Object
	var err error

	bucketName := mux.Vars(r)["bucket"]

	queryParams := r.URL.Query()
	listType, _ := strconv.Atoi(queryParams.Get("list-type"))
	prefix := queryParams.Get("prefix")
	maxKeys, _ := strconv.Atoi(queryParams.Get("max-keys"))

	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
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
			objects, err = s.ListObjectsV2Recursive(bucketName, prefix, maxKeys)
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

func (s *ltosServer) handleObjectCmds(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]

	if !s.BucketExists(bucketName) {
		s.sendErrorResponse(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	isDir := false
	if strings.HasSuffix(objectKey, "/") {
		isDir = true
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
		w.Header().Set("Content-Length", strconv.FormatInt(metadata.Size, 10))
		w.Header().Set("ETag", metadata.ETag)
		lastModified := metadata.LastModified.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("Last-Modified", lastModified)

		if isDir {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Get the object data
		data, err := s.GetObject(bucketName, objectKey)
		if err != nil {
			log.Printf("Error getting object data: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		// Write the data to the response
		_, err = w.Write(data)
		if err != nil {
			log.Printf("Error writing response: %v", err)
		}
	case "PUT":
		if isDir {
			s.createDirectoryAndCotrolFiles(bucketName, objectKey)
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
		err = s.PutObject(bucketName, objectKey, body, metadata, isDir)
		if err != nil {
			log.Printf("Error storing object: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		err = s.replicateObject(bucketName, objectKey, body)
		if err != nil {
			log.Printf("Failed to replicate object %s/%s: %v", bucketName, objectKey, err)
			// Optionally handle the replication failure
		}
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

			ltosServer := newLiteObjectStoreServer(filesystemDrive, filesystemReplicaDrive)

			r := mux.NewRouter()

			// Apply the authentication middleware to all routes
			r.Use(authMiddleware(accessKeyID, secretAccessKey))

			// Define routes

			r.HandleFunc("/", ltosServer.handleRoot).Methods("GET")
			r.HandleFunc("/{bucket}", ltosServer.handleBucketCmds).Methods("GET", "PUT", "DELETE")
			r.HandleFunc("/{bucket}/{object:.+}", ltosServer.handleObjectCmds).Methods("GET", "PUT", "DELETE", "HEAD")
			r.HandleFunc("/{bucket}/{object:.+}", ltosServer.handleNewMultipartUpload).
				Methods("POST").
				Queries("uploads", "")
			r.HandleFunc("/{bucket}/{object:.+}", ltosServer.handleUploadPart).
				Methods("PUT").
				Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId}")
			r.HandleFunc("/{bucket}/{object:.+}", ltosServer.handleCompleteMultipartUpload).
				Methods("POST").
				Queries("uploadId", "{uploadId}")
			r.HandleFunc("/{bucket}/", ltosServer.handleBucketCmdsLocation).
				Methods("GET").
				Queries("location", "")

			r.PathPrefix("/").Handler(loggingMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			})))

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
		log.Printf("Received request: %s %s", r.Method, r.URL)
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
	PutObject(bucket string, obj string, data []byte, metadata map[string]string, sDir bool) error
	InitiateMultipartUpload(bucket, key string) (string, error)
	PutObjectPart(bucket, key, uploadID string, partNumber int, data []byte) error
	CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) error
	IsPrefix(bucket, prefix string) (bool, error)
	ListObjectsV2(bucket, prefix string, maxKeys int) ([]Object, error)
}

func (s *ltosServer) IsPrefix(bucket, prefix string) (bool, error) {
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

func (s *ltosServer) BucketExists(name string) bool {
	_, err := os.Stat(filepath.Join(s.dataDir, name))
	return !os.IsNotExist(err)
}

func (s *ltosServer) BucketExistsReplica(name string) bool {
	_, err := os.Stat(filepath.Join(s.dataDirReplica, name))
	return !os.IsNotExist(err)
}

func (s *ltosServer) ListObjects(bucket string) ([]Object, error) {
	bucketPath := filepath.Join(s.dataDir, bucket)
	var objects []Object

	err := filepath.Walk(bucketPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory
		if path == bucketPath {
			return nil
		}
		if isHidden(path) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Get the key (path relative to bucket root)
		key, err := filepath.Rel(bucketPath, path)
		if err != nil {
			return err
		}
		key = filepath.ToSlash(key) // Convert to forward slashes for S3 compatibility

		if !info.IsDir() {
			content, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}
			hash := md5.Sum(content)
			objects = append(objects, Object{
				Key:          key,
				LastModified: info.ModTime(),
				ETag:         fmt.Sprintf("\"%x\"", hash),
				Size:         info.Size(),
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return objects, nil
}

func (s *ltosServer) ListBuckets() ([]Bucket, error) {
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

func (s *ltosServer) sendErrorResponse(w http.ResponseWriter, code string, status int) {
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

func (s *ltosServer) GetObjectMetadata(bucket, key string, contentType string) (*ObjectMetadata, error) {
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

	if !info.IsDir() {
		content, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}
		hash := md5.Sum(content)
		metadata.ETag = fmt.Sprintf("\"%x\"", hash)
	}

	return metadata, nil
}

func (s *ltosServer) PutObject(bucket, key string, data []byte, metadata map[string]string, isDir bool) error {
	fullPath := filepath.Join(s.dataDir, bucket, key)

	// Ensure all parent directories exist and create control files for them
	err := s.createDirectoriesAndControlFiles(s.dataDir, bucket, key, isDir)
	if err != nil {
		return err
	}
	err = s.createDirectoriesAndControlFiles(s.dataDirReplica, bucket, key, isDir)
	if err != nil {
		return err
	}

	// If the key ends with a slash, it's a directory
	if strings.HasSuffix(key, "/") {
		// The directory and its control file have already been created
		return nil
	}
	// For regular files
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

func (s *ltosServer) createDirectoriesAndControlFiles(dataDir, bucket, objectKey string, isDir bool) error {
	fullPath := filepath.Join(s.dataDir, bucket, objectKey)
	if isDir {
		os.MkdirAll(fullPath, 0755)
	} else {
		os.MkdirAll(filepath.Dir(fullPath), 0755)
	}

	s.mu.Lock()
	_, exists := s.controlFileMap[fullPath]
	if exists {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

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
	s.mu.Lock()
	s.controlFileMap[fullPath] = true
	s.mu.Unlock()
	return nil
}

func (s *ltosServer) handleMultipartUpload(w http.ResponseWriter, r *http.Request) {
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

func (s *ltosServer) InitiateMultipartUpload(bucket, key string) (string, error) {
	uploadID := generateUploadID()
	uploadDir := filepath.Join(s.dataDir, bucket, fmt.Sprintf("%s.%s", key, uploadID))
	err := os.MkdirAll(uploadDir, 0755)
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

func (s *ltosServer) initiateMultipartUpload(w http.ResponseWriter, r *http.Request, bucketName, objectKey string) {
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

func (s *ltosServer) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
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

func (s *ltosServer) handleNewMultipartUpload(w http.ResponseWriter, r *http.Request) {
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

func (s *ltosServer) handleUploadPart(w http.ResponseWriter, r *http.Request) {
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

func (s *ltosServer) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
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

func (s *ltosServer) PutObjectPart(bucket, key, uploadID string, partNumber int, data []byte) error {
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

func (s *ltosServer) CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) error {
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

func (s *ltosServer) replicateObject(bucketName, key string, data []byte) error {
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
	s.createCtrFile(s.dataDirReplica, bucketName, key)

	return nil
}

func (s *ltosServer) replicateObjectFileCopy(bucketName, key string) error {
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

func (s *ltosServer) copyFile(src, dst string) error {
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

func (s *ltosServer) handleHeadCmdBucket(w http.ResponseWriter, r *http.Request, bucketName string) error {
	path := filepath.Join(s.dataDir, bucketName)
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Bucket not found", http.StatusNotFound)
		} else {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return err
	}

	if !info.IsDir() {
		http.Error(w, "Not a bucket", http.StatusBadRequest)
		return fmt.Errorf("not a bucket: %s", bucketName)
	}

	// Set Last-Modified header
	lastModified := info.ModTime().UTC().Format(time.RFC1123)
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

func (s *ltosServer) handleHeadObject(w http.ResponseWriter, r *http.Request, bucketName, objectKey string) {
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

func isHidden(path string) bool {
	name := filepath.Base(path)

	if strings.HasPrefix(name, ".") {
		return true
	}

	return false
}

func (s *ltosServer) ListObjectsV2Recursive(bucket, prefix string, maxKeys int) ([]Object, error) {
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

		if isHidden(path) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		relPath = filepath.ToSlash(relPath)
		if prefix == "" || strings.HasPrefix(relPath, prefix) {
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
			return filepath.SkipDir
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return objects, nil
}

func (s *ltosServer) ListObjectsV2(bucket, prefix string, maxKeys int) ([]Object, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	bucketPath := filepath.Join(s.dataDir, bucket)
	prefixPath := filepath.Join(bucketPath, prefix)

	var objects []Object
	var count int
	// Read the directory contents
	entries, err := ioutil.ReadDir(prefixPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // Return empty list if directory doesn't exist
		}
		return nil, err
	}

	for _, entry := range entries {
		relPath := entry.Name()
		if strings.HasPrefix(relPath, ".") && strings.HasSuffix(relPath, ".metadata") {
			continue
		}

		object := Object{
			Key:          relPath,
			LastModified: entry.ModTime(),
			ETag:         fmt.Sprintf("\"%x\"", md5.Sum([]byte(relPath))), // Simple ETag for demo
			Size:         entry.Size(),
			IsDirectory:  entry.IsDir(),
		}

		if entry.IsDir() {
			object.Key = object.Key + "/"
			object.Size = 0
		}

		objects = append(objects, object)
		count++

		if maxKeys != 0 && count >= maxKeys {
			break
		}
	}

	// If no objects found in the prefix directory, list the bucket root
	if len(objects) == 0 && prefix != "" {
		return s.ListObjectsV2(bucket, "", maxKeys)
	}

	return objects, nil
}

func (s *ltosServer) ListBucket(bucket string) ([]Object, error) {
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
		// This is a simplified example; you'll need to implement proper parsing
		authInfo["type"] = "header"
		authInfo["auth"] = auth
	} else {
		// Check query parameters
		authInfo["type"] = "query"
		authInfo["accessKey"] = r.URL.Query().Get("AWSAccessKeyId")
		authInfo["signature"] = r.URL.Query().Get("Signature")
		// Add other necessary query parameters
	}

	return authInfo
}

func validateAuth(authInfo map[string]string, accessKeyID, secretAccessKey string) bool {
	// This is a placeholder implementation
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
func (s *ltosServer) replicateObjectFile(sourceBucket, replicaBucket, object string, data io.Reader) error {
	content, err := ioutil.ReadAll(data)
	if err != nil {
		return fmt.Errorf("failed to read object data: %v", err)
	}

	metadata := make(map[string]string)

	return s.PutObject(replicaBucket, object, content, metadata, false)
}

func (rm *ReplicationManager) RemoveReplica(sourceBucket string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	delete(rm.replicas, sourceBucket)
}

func (s *ltosServer) deleteBucket(bucket string) error {
	bucketPath := filepath.Join(s.dataDir, bucket)
	err := s.DeleteBucket(bucketPath)
	if err == nil {
		bucketPath = filepath.Join(s.dataDirReplica, bucket)
		err = s.DeleteBucket(bucketPath)
	}
	return err
}

func (s *ltosServer) DeleteBucket(bucketPath string) error {
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

func (s *ltosServer) handleBucketCmdsLocation(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket := vars["bucket"]

	// Check if the bucket exists
	// This is a placeholder - you'll need to implement actual bucket existence checking
	if !s.BucketExists(bucket) {
		s.sendErrorResponse(w, "NoSuchBucket", http.StatusNotFound)
		return
	}

	// Get the bucket location
	// This is a placeholder - you'll need to implement actual location retrieval
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

func (s *ltosServer) getBucketLocation(bucket string) (string, error) {
	// This is a placeholder implementation
	// In a real scenario, you would look up the bucket's location in your storage system
	return "us-east-1", nil
}

func (s *ltosServer) createDirectoryAndCotrolFilesTmp(bucket, objectKey string) error {
	err := s.createDirectoryAndControlFile(s.dataDir, bucket, objectKey)
	if err == nil {
		err = s.createDirectoryAndControlFile(s.dataDirReplica, bucket, objectKey)
	}
	return err
}

func (s *ltosServer) createDirectoryAndControlFileTmp(dataDir, bucket, objectKey string) error {
	fullPath := filepath.Join(dataDir, bucket, objectKey)
	os.MkdirAll(fullPath, 0755)

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

func (s *ltosServer) createCtrFile(dataDir, bucket, objectKey string) error {
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

func (s *ltosServer) ListObjectsV2RecursiveTmp(bucket, prefix string, maxKeys int) ([]Object, error) {
	bucketPath := filepath.Join(s.dataDir, bucket)
	prefixPath := filepath.Join(bucketPath, prefix)

	var objects []Object
	var count int

	err := filepath.Walk(prefixPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(bucketPath, path)
		if err != nil {
			return err
		}

		relPath = filepath.ToSlash(relPath)

		if !strings.HasPrefix(relPath, prefix) {
			return nil
		}

		if isHidden(info.Name()) {
			return nil
		}

		// Check for .metadata file
		metadataPath := filepath.Join(filepath.Dir(path), "."+info.Name()+".metadata")
		if _, err := os.Stat(metadataPath); os.IsNotExist(err) {
			return nil
		}

		relPath, _ = filepath.Rel(bucketPath, path)
		key := relPath
		if info.IsDir() {
			key += "/"
		}

		objects = append(objects, Object{
			Key:          key,
			LastModified: info.ModTime(),
			ETag:         fmt.Sprintf("\"%x\"", md5.Sum([]byte(key))),
			Size:         info.Size(),
			IsDirectory:  info.IsDir(),
		})

		count++
		if maxKeys != 0 && count >= maxKeys {
			return filepath.SkipDir
		}

		return nil
	})

	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return objects, nil
}

func (s *ltosServer) createDirectoryAndCotrolFiles(bucket, objectKey string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := s.createDirectoryAndControlFile(s.dataDir, bucket, objectKey); err != nil {
			errChan <- fmt.Errorf("main dir: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := s.createDirectoryAndControlFile(s.dataDirReplica, bucket, objectKey); err != nil {
			errChan <- fmt.Errorf("replica dir: %v", err)
		}
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		return err // Return the first error encountered
	}

	return nil
}

func (s *ltosServer) createDirectoryAndControlFile(dataDir, bucket, objectKey string) error {
	fullPath := filepath.Join(dataDir, bucket, objectKey)
	os.MkdirAll(fullPath, 0755)

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
		log.Printf("Created metadata file: %s", fullPath)
	}
	return nil
}
