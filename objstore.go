package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/urfave/cli/v2"
)

const (
	objectStoreDataDir = "/mnt/objsdata/"
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
}

type FileSystemBackend struct {
	dataDir string
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

type Bucket struct {
	Name         string
	CreationDate time.Time
}

type S3Server struct {
	storage StorageBackend
}

func NewFileSystemBackend(dataDir string) (*FileSystemBackend, error) {
	err := newFileSystemBackend(dataDir)
	if err != nil {
		return nil, err
	}
	return &FileSystemBackend{dataDir: dataDir}, nil
}

func newFileSystemBackend(dataDir string) error {
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		err := os.MkdirAll(dataDir, 0755)
		if err != nil {
			fmt.Printf("Failed to create directory, please create directory %v\n", dataDir)
			return fmt.Errorf("failed to create base data directory: %v", err)
		}
	}

	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		fmt.Printf("Failed to create directory, please create directory %v as root\n", dataDir)
		return fmt.Errorf("base data directory does not exist after creation attempt: %v", err)
	}
	return nil
}

// Create buket if not existing
func (fs *FileSystemBackend) CreateBucket(name string) error {
	if fs.BucketExists(name) {
		return nil
	}
	return os.Mkdir(filepath.Join(fs.dataDir, name), 0755)
}

func (fs *FileSystemBackend) DeleteBucket(name string) error {
	return os.RemoveAll(filepath.Join(fs.dataDir, name))
}

func (fs *FileSystemBackend) GetObject(bucket, key string) ([]byte, error) {
	path := filepath.Join(fs.dataDir, bucket, key)
	return ioutil.ReadFile(path)
}

func (fs *FileSystemBackend) DeleteObject(bucket, key string) error {
	path := filepath.Join(fs.dataDir, bucket, key)
	return os.RemoveAll(path)
}

func NewS3Server(storage StorageBackend) *S3Server {
	return &S3Server{storage: storage}
}
func (s *S3Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/xml")

	buckets, err := s.storage.ListBuckets()
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
			DisplayName: "S3ServerOwner",
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

func (s *S3Server) handleBucket(w http.ResponseWriter, r *http.Request) {
	var objects []Object
	var err error
	bucketName := mux.Vars(r)["bucket"]

	queryParams := r.URL.Query()
	listType, _ := strconv.Atoi(queryParams.Get("list-type"))
	prefix := queryParams.Get("prefix")
	maxKeys, _ := strconv.Atoi(queryParams.Get("max-keys"))

	switch r.Method {
	case "GET":
		if !s.storage.BucketExists(bucketName) {
			s.sendErrorResponse(w, "NoSuchBucket", http.StatusNotFound)
			return
		}
		if listType == 2 {
			objects, err = s.storage.ListObjectsV2(bucketName, prefix, maxKeys)
		} else {
			objects, err = s.storage.ListObjects(bucketName)
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
			IsTruncated bool     `xml:"IsTruncated"`
			Contents    []struct {
				Key          string `xml:"Key"`
				LastModified string `xml:"LastModified"`
				ETag         string `xml:"ETag"`
				Size         int64  `xml:"Size"`
				StorageClass string `xml:"StorageClass"`
			} `xml:"Contents"`
		}{
			Xmlns:       "http://s3.amazonaws.com/doc/2006-03-01/",
			Name:        bucketName,
			Prefix:      "",
			MaxKeys:     1000,
			IsTruncated: false,
		}

		for _, obj := range objects {
			response.Contents = append(response.Contents, struct {
				Key          string `xml:"Key"`
				LastModified string `xml:"LastModified"`
				ETag         string `xml:"ETag"`
				Size         int64  `xml:"Size"`
				StorageClass string `xml:"StorageClass"`
			}{
				Key:          obj.Key,
				LastModified: obj.LastModified.Format(time.RFC3339),
				ETag:         obj.ETag,
				Size:         obj.Size,
				StorageClass: "STANDARD",
			})
		}
		response.KeyCount = len(response.Contents)

		w.Header().Set("Content-Type", "application/xml")
		enc := xml.NewEncoder(w)
		enc.Indent("", "  ")
		if err := enc.Encode(response); err != nil {
			log.Printf("Error encoding response: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		}

	case "PUT":
		if !isValidBucketName(bucketName) {
			s.sendErrorResponse(w, "InvalidBucketName", http.StatusBadRequest)
			return
		}
		err := s.storage.CreateBucket(bucketName)
		if err != nil {
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Location", "/"+bucketName)
	case "DELETE":
		err := s.storage.DeleteBucket(bucketName)
		if err != nil {
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}

}

func (s *S3Server) handleObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]

	// Remove the leading slash if it exists
	objectKey = strings.TrimPrefix(objectKey, "/")
	switch r.Method {
	case "HEAD":
		s.handleHeadObject(w, r, bucketName, objectKey)
	case "GET":
		// Existing GET logic
		data, err := s.storage.GetObject(bucketName, objectKey)
		if err != nil {
			if os.IsNotExist(err) {
				s.sendErrorResponse(w, "NoSuchKey", http.StatusNotFound)
			} else {
				s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			}
			return
		}
		w.Write(data)
	case "PUT":
		vars := mux.Vars(r)
		bucketName := vars["bucket"]
		objectKey := vars["object"]

		// Ensure the bucket exists
		if !s.storage.BucketExists(bucketName) {
			s.sendErrorResponse(w, "NoSuchBucket", http.StatusNotFound)
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
		err = s.storage.PutObject(bucketName, objectKey, body, metadata)
		if err != nil {
			log.Printf("Error storing object: %v", err)
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		// Send success response
		w.WriteHeader(http.StatusOK)
		// Existing PUT logic
		// ...
	case "DELETE":
		err := s.storage.DeleteObject(bucketName, objectKey)
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
		},
		Action: func(c *cli.Context) error {
			urlStr := c.String("url")
			//accessKeyID := c.String("access-key-id")
			//secretAccessKey := c.String("secret-access-key")
			filesystemDrive := c.String("filesystem-drive")
			parsedURL, err := url.Parse(urlStr)
			if err != nil {
				return fmt.Errorf("invalid URL: %w", err)
			}

			// Determine the port to use
			port := parsedURL.Port()
			if port == "" {
				port = "50051" // Default port if none specified
			}

			// Initialize the backend and server
			backend, err := NewFileSystemBackend(filesystemDrive)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			s3server := NewS3Server(backend)

			r := mux.NewRouter()

			// Define routes
			r.HandleFunc("/", s3server.handleRoot).Methods("GET")
			r.HandleFunc("/{bucket}", s3server.handleBucket).Methods("GET", "PUT", "DELETE")
			r.HandleFunc("/{bucket}/{object:.+}", s3server.handleNewMultipartUpload).
				Methods("POST").
				Queries("uploads", "")
			r.HandleFunc("/{bucket}/{object:.+}", s3server.handleUploadPart).
				Methods("PUT").
				Queries("partNumber", "{partNumber:[0-9]+}", "uploadId", "{uploadId}")
			r.HandleFunc("/{bucket}/{object:.+}", s3server.handleCompleteMultipartUpload).
				Methods("POST").
				Queries("uploadId", "{uploadId}")
			r.HandleFunc("/{bucket}/{object:.+}", s3server.handleObject).Methods("GET", "PUT", "DELETE", "HEAD")

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
	GetObject(bucket, key string) ([]byte, error)
	DeleteObject(bucket, key string) error
	BucketExists(name string) bool
	GetObjectMetadata(bucket, key string) (*ObjectMetadata, error)
	PutObject(bucket string, obj string, data []byte, metadata map[string]string) error
	InitiateMultipartUpload(bucket, key string) (string, error)
	PutObjectPart(bucket, key, uploadID string, partNumber int, data []byte) error
	CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) error
	IsPrefix(bucket, prefix string) (bool, error)
	ListObjectsV2(bucket, prefix string, maxKeys int) ([]Object, error)
}

func (fs *FileSystemBackend) IsPrefix(bucket, prefix string) (bool, error) {
	dirPath := filepath.Join(fs.dataDir, bucket, prefix)
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return len(files) > 0, nil
}

func (fs *FileSystemBackend) BucketExists(name string) bool {
	_, err := os.Stat(filepath.Join(fs.dataDir, name))
	return !os.IsNotExist(err)
}

func (fs *FileSystemBackend) ListObjects(bucket string) ([]Object, error) {
	bucketPath := filepath.Join(fs.dataDir, bucket)
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

func (fs *FileSystemBackend) ListBuckets() ([]Bucket, error) {
	files, err := ioutil.ReadDir(fs.dataDir)
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

func (s *S3Server) sendErrorResponse(w http.ResponseWriter, code string, status int) {
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

func (fs *FileSystemBackend) GetObjectMetadata(bucket, key string) (*ObjectMetadata, error) {
	path := filepath.Join(fs.dataDir, bucket, key)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	metadata := &ObjectMetadata{
		Size:         info.Size(),
		LastModified: info.ModTime(),
		IsDirectory:  info.IsDir(),
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

func (fs *FileSystemBackend) PutObject(bucket, key string, data []byte, metadata map[string]string) error {
	fullPath := filepath.Join(fs.dataDir, bucket, key)

	// Ensure all parent directories exist
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}
	// If the key ends with a slash, it's a directory
	if strings.HasSuffix(key, "/") {
		// For directories, just ensure it exists and return
		return nil
	}

	// Write the file
	if err := ioutil.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	// Store metadata (you might want to implement a separate metadata storage system)
	metadataPath := fullPath + ".metadata"
	metadataPath = filepath.Join(filepath.Dir(metadataPath), fmt.Sprintf(".%v", filepath.Base(metadataPath)))
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}
	if err := ioutil.WriteFile(metadataPath, metadataJSON, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %v", err)
	}

	return nil
}

func (s *S3Server) handleMultipartUpload(w http.ResponseWriter, r *http.Request) {
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

func (fs *FileSystemBackend) InitiateMultipartUpload(bucket, key string) (string, error) {
	uploadID := generateUploadID()
	uploadDir := filepath.Join(fs.dataDir, bucket, fmt.Sprintf("%s.%s", key, uploadID))
	err := os.MkdirAll(uploadDir, 0755)
	if err != nil {
		return "", err
	}
	return uploadID, nil
}

func (s *S3Server) initiateMultipartUpload(w http.ResponseWriter, r *http.Request, bucketName, objectKey string) {
	// Generate upload ID
	uploadID, err := s.storage.InitiateMultipartUpload(bucketName, objectKey)
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

func (s *S3Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	log.Println("handleCreateMultipartUpload function called")

	vars := mux.Vars(r)
	log.Printf("vars: %+v\n", vars)

	bucketName := vars["bucket"]
	objectKey := vars["key"]
	log.Printf("bucketName: %s, objectKey: %s\n", bucketName, objectKey)

	// Log query parameters
	log.Printf("Query params: %v\n", r.URL.Query())

	// Initiate the multipart upload
	uploadID, err := s.storage.InitiateMultipartUpload(bucketName, objectKey)
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

func (s *S3Server) handleNewMultipartUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]

	// Generate a unique upload ID
	//uploadID := generateUploadID()
	// Store initial multipart upload info in your backend
	uploadID, err := s.storage.InitiateMultipartUpload(bucketName, objectKey)
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

func (s *S3Server) handleUploadPart(w http.ResponseWriter, r *http.Request) {
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
	err = s.storage.PutObjectPart(bucketName, objectKey, uploadID, partNumber, body)
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

func (s *S3Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["object"]
	uploadID := vars["uploadId"]

	var completeMultipartUpload CompleteMultipartUpload
	if err := xml.NewDecoder(r.Body).Decode(&completeMultipartUpload); err != nil {
		s.sendErrorResponse(w, "MalformedXML", http.StatusBadRequest)
		return
	}

	err := s.storage.CompleteMultipartUpload(bucketName, objectKey, uploadID, completeMultipartUpload.Parts)
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

func (fs *FileSystemBackend) PutObjectPart(bucket, key, uploadID string, partNumber int, data []byte) error {
	// Create the directory for this multipart upload if it doesn't exist
	uploadDir := filepath.Join(fs.dataDir, bucket, fmt.Sprintf("%s.%s", key, uploadID))
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

func (fs *FileSystemBackend) CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart) error {
	uploadDir := filepath.Join(fs.dataDir, bucket, fmt.Sprintf("%s.%s", key, uploadID))
	finalPath := filepath.Join(fs.dataDir, bucket, key)

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

	// Clean up the upload directory
	if err := os.RemoveAll(uploadDir); err != nil {
		// Log this error, but don't fail the operation
		log.Printf("Warning: failed to remove upload directory: %v", err)
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

func (s *S3Server) handleHeadObject(w http.ResponseWriter, r *http.Request, bucketName, objectKey string) {
	//log.Printf("Handling HEAD request for bucket: %s, key: %s", bucketName, objectKey)

	metadata, err := s.storage.GetObjectMetadata(bucketName, objectKey)
	if err != nil {
		if os.IsNotExist(err) {
			s.sendErrorResponse(w, "NoSuchKey", http.StatusNotFound)
		} else {
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", metadata.Size))
	w.Header().Set("Last-Modified", metadata.LastModified.Format(http.TimeFormat))
	w.Header().Set("ETag", metadata.ETag)

	if metadata.IsDirectory {
		w.Header().Set("X-Is-Directory", "true")
	} else {
		w.Header().Set("X-Is-Directory", "false")
	}

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

func (fs *FileSystemBackend) ListObjectsV2(bucket, prefix string, maxKeys int) ([]Object, error) {
	bucketPath := filepath.Join(fs.dataDir, bucket)
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

		relPath = filepath.ToSlash(relPath)

		if strings.HasPrefix(relPath, prefix) {
			if info.IsDir() {
				if relPath != prefix && relPath != "." {
					objects = append(objects, Object{
						Key:          relPath + "/",
						LastModified: info.ModTime(),
						ETag:         "",
						Size:         0,
						IsDirectory:  true,
					})
					count++
				}
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

		if count >= maxKeys {
			return filepath.SkipDir
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if len(objects) == 0 {
		files, err := os.ReadDir(bucketPath)
		if err != nil {
			return nil, err
		}
		for _, file := range files {
			if strings.HasPrefix(file.Name(), prefix) {
				relPath := filepath.ToSlash(file.Name())
				info, err := file.Info()
				if err != nil {
					continue
				}
				objects = append(objects, Object{
					Key:          relPath,
					LastModified: info.ModTime(),
					ETag:         fmt.Sprintf("\"%x\"", md5.Sum([]byte(relPath))), // Simple ETag for demo
					Size:         info.Size(),
					IsDirectory:  file.IsDir(),
				})
				count++
				if count >= maxKeys {
					break
				}
			}
		}
	}

	//fmt.Printf("ListObjectsV2: Found %d objects\n", len(objects))
	//for _, obj := range objects {
		//fmt.Printf("  - Key: %s, IsDirectory: %v\n", obj.Key, obj.IsDirectory)
	//}
	return objects, nil
}
