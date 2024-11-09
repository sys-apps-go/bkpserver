package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
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

type SyncManager struct {
	watcher       *fsnotify.Watcher
	watchedPaths  map[string]bool
	mu            sync.Mutex
	syncInterval  time.Duration
	lastSyncTimes map[string]time.Time
}

type ObjectLockConfiguration struct {
	XMLName           xml.Name `xml:"ObjectLockConfiguration"`
	ObjectLockEnabled string   `xml:"ObjectLockEnabled"`
	Rule              *Rule    `xml:"Rule,omitempty"`
}

type Rule struct {
	DefaultRetention *DefaultRetention `xml:"DefaultRetention"`
}

type DefaultRetention struct {
	Mode  string `xml:"Mode"`
	Days  int    `xml:"Days,omitempty"`
	Years int    `xml:"Years,omitempty"`
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
	Events       []Event
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
	storage                 StorageBackend
	dataDir                 string
	dataDirReplica          string
	fileSystemReplName      string
	bucketNames             map[string]bool
	multipartUploads        sync.Map
	replicationManager      *ReplicationManager
	metadataIndex           map[string]MetadataIndex
	bucketReplication       map[string]string
	bucketReplicationConfig map[string]ReplicationConfig
	syncManager             *SyncManager
	mu                      sync.Mutex
	objectLocks             map[string]*ObjectLockConfiguration
	objectLockMutex         sync.RWMutex
	objectMutexes           map[string]*sync.Mutex
	objectMutexLock         sync.Mutex
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

func (s *objStoreServer) DeleteBucketReplica(name string) error {
	if !s.BucketExistsReplica(name) {
		return nil
	}
	return os.RemoveAll(filepath.Join(s.dataDirReplica, name))
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
		dataDir:                 primaryFs,
		dataDirReplica:          replicaFs,
		replicationManager:      NewReplicationManager(),
		bucketNames:             make(map[string]bool),
		metadataIndex:           make(map[string]MetadataIndex),
		bucketReplication:       make(map[string]string),
		bucketReplicationConfig: make(map[string]ReplicationConfig),
		objectLocks:             make(map[string]*ObjectLockConfiguration),
		objectMutexes:           make(map[string]*sync.Mutex),
	}
	s.syncManager, err = NewSyncManager(5 * time.Second)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return s
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
			r.HandleFunc("/{bucket}", objStoreServer.handlePutBucketReplication).
				Methods("PUT", "GET", "DELETE").
				Queries("replication", "")
			r.HandleFunc("/{bucket}/", objStoreServer.handleBucketCmdsLocation).
				Methods("GET").
				Queries("location", "")
			r.HandleFunc("/{bucket}", objStoreServer.handleBucketEventNotification).
				Methods("PUT", "GET", "DELETE").
				Queries("notification", "")
			r.HandleFunc("/{bucket}/events", objStoreServer.listenBucketEvents).
				Methods("GET").
				Queries("listen", "")

			r.HandleFunc("/{bucket}", objStoreServer.handleGetBucketObjectLock).Methods("GET").Queries("object-lock", "")
			r.HandleFunc("/{bucket}", objStoreServer.handlePutBucketObjectLock).Methods("PUT").Queries("object-lock", "")

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

func splitBaseDir(path string) (string, string) {
	lastIndex := strings.LastIndex(path, "/")
	if lastIndex != -1 {
		return path[lastIndex+1:], path[:lastIndex]
	}

	return path, ""
}

func (s *objStoreServer) getBucketLocation(bucket string) (string, error) {
	return "us-east-1", nil
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
