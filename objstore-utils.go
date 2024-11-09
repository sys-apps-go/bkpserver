package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

func (s *objStoreServer) handleSyncRequest(w http.ResponseWriter, r *http.Request) {
	bucketName := mux.Vars(r)["bucket"]
	fileName := r.URL.Query().Get("file")

	if bucketName == "" || fileName == "" {
		http.Error(w, "Missing bucket or file parameter", http.StatusBadRequest)
		return
	}

	localPath := filepath.Join(s.dataDir, bucketName, fileName)

	err := s.syncManager.AddPath(localPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to set up sync: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Sync initiated for %s/%s", bucketName, fileName)
}

type Event struct {
	Type string `json:"type"`
	// Add other event properties as needed
}

var (
	buckets = make(map[string]*Bucket)
	mu      sync.RWMutex
)

// SetEvent handler
func SetEvent(w http.ResponseWriter, r *http.Request) {
	bucketName := r.URL.Query().Get("bucket")
	if bucketName == "" {
		http.Error(w, "Bucket name is required", http.StatusBadRequest)
		return
	}

	var event Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "Invalid event data", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	bucket, exists := buckets[bucketName]
	if !exists {
		bucket = &Bucket{Name: bucketName}
		buckets[bucketName] = bucket
	}

	bucket.Events = append(bucket.Events, event)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"message": "Event set successfully"})
}

// GetEventList handler
func GetEventList(w http.ResponseWriter, r *http.Request) {
	bucketName := r.URL.Query().Get("bucket")
	if bucketName == "" {
		http.Error(w, "Bucket name is required", http.StatusBadRequest)
		return
	}

	mu.RLock()
	defer mu.RUnlock()

	bucket, exists := buckets[bucketName]
	if !exists {
		http.Error(w, "Bucket not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(bucket.Events)
}

// RemoveEvent handler
func RemoveEvent(w http.ResponseWriter, r *http.Request) {
	bucketName := r.URL.Query().Get("bucket")
	eventType := r.URL.Query().Get("type")
	if bucketName == "" || eventType == "" {
		http.Error(w, "Bucket name and event type are required", http.StatusBadRequest)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	bucket, exists := buckets[bucketName]
	if !exists {
		http.Error(w, "Bucket not found", http.StatusNotFound)
		return
	}

	for i, event := range bucket.Events {
		if event.Type == eventType {
			bucket.Events = append(bucket.Events[:i], bucket.Events[i+1:]...)
			break
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Event removed successfully"})
}

// ListenEvent handler (this is a simplified example using Server-Sent Events)
func ListenEvent(w http.ResponseWriter, r *http.Request) {
	bucketName := r.URL.Query().Get("bucket")
	if bucketName == "" {
		http.Error(w, "Bucket name is required", http.StatusBadRequest)
		return
	}

	// Set headers for SSE
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Create a channel for new events
	eventChan := make(chan Event)

	// Start a goroutine to listen for new events
	go func() {
		for {
			mu.RLock()
			bucket, exists := buckets[bucketName]
			if exists && len(bucket.Events) > 0 {
				event := bucket.Events[len(bucket.Events)-1]
				eventChan <- event
			}
			mu.RUnlock()
			// Sleep to avoid tight looping
			time.Sleep(1 * time.Second)
		}
	}()

	// Write events to the response
	for event := range eventChan {
		data, _ := json.Marshal(event)
		fmt.Fprintf(w, "data: %s\n\n", data)
		w.(http.Flusher).Flush()
	}
}

type ObjectStoreServer struct {
}

type EventNotification struct {
	Events []string `json:"Events"`
	// Add other fields as needed, such as ARN for SNS/SQS, etc.
}

func (s *ObjectStoreServer) handleRoot(w http.ResponseWriter, r *http.Request) {
	// Handle the root path "/"
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Welcome to the Object Store Server"))
}

func (s *objStoreServer) handleBucketEventNotification(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	switch r.Method {
	case "PUT":
		s.setBucketEventNotification(w, r, bucketName)
	case "GET":
		if r.URL.Query().Get("listen") != "" {
			s.listenBucketEvents(w, r)
		} else {
			s.getBucketEventNotification(w, r, bucketName)
		}
	case "DELETE":
		s.deleteBucketEventNotification(w, r, bucketName)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *objStoreServer) setBucketEventNotification(w http.ResponseWriter, r *http.Request, bucketName string) {
	var notification EventNotification
	if err := json.NewDecoder(r.Body).Decode(&notification); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Implement logic to set the event notification for the bucket
	// This might involve storing the configuration in a database or in-memory store

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Event notification set successfully"})
}

func (s *objStoreServer) getBucketEventNotification(w http.ResponseWriter, r *http.Request, bucketName string) {
	// Implement logic to retrieve the event notification configuration for the bucket
	// This might involve fetching from a database or in-memory store

	// For this example, we'll use a dummy notification
	notification := EventNotification{
		Events: []string{"s3:ObjectCreated:*", "s3:ObjectRemoved:*"},
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(notification)
}

func (s *objStoreServer) deleteBucketEventNotification(w http.ResponseWriter, r *http.Request, bucketName string) {
	// Implement logic to delete the event notification configuration for the bucket
	// This might involve removing the configuration from a database or in-memory store

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Event notification deleted successfully"})
}

func (s *objStoreServer) listenBucketEvents(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	// Set headers for Server-Sent Events (SSE)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Create a channel for events
	events := make(chan string)
	fmt.Println("Listen on events: ", bucketName)
	// In a real implementation, you would register this channel
	// to receive events for the specified bucket

	// Keep the connection open
	for {
		select {
		case event := <-events:
			// Send the event to the client
			fmt.Fprintf(w, "data: %s\n\n", event)
			w.(http.Flusher).Flush()
		case <-r.Context().Done():
			// Client disconnected
			return
		}
	}
}

func (s *objStoreServer) handleGetBucketObjectLock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	s.objectLockMutex.RLock()
	lockConfig, exists := s.objectLocks[bucketName]
	s.objectLockMutex.RUnlock()

	if exists {
		http.Error(w, "Object lock configuration not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	encoder := xml.NewEncoder(w)
	if err := encoder.Encode(lockConfig); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

func (s *objStoreServer) handlePutBucketObjectLock(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]

	var config ObjectLockConfiguration
	if err := xml.NewDecoder(r.Body).Decode(&config); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if err := s.validateObjectLockConfig(&config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.objectLockMutex.Lock()
	s.objectLocks[bucketName] = &config
	s.objectLockMutex.Unlock()

	w.WriteHeader(http.StatusOK)
}

func (s *objStoreServer) validateObjectLockConfig(config *ObjectLockConfiguration) error {
	if config.ObjectLockEnabled != "Enabled" {
		return fmt.Errorf("ObjectLockEnabled must be 'Enabled'")
	}
	if config.Rule != nil && config.Rule.DefaultRetention != nil {
		if config.Rule.DefaultRetention.Mode != "GOVERNANCE" && config.Rule.DefaultRetention.Mode != "COMPLIANCE" {
			return fmt.Errorf("Invalid retention mode")
		}
		if (config.Rule.DefaultRetention.Days > 0 && config.Rule.DefaultRetention.Years > 0) ||
			(config.Rule.DefaultRetention.Days == 0 && config.Rule.DefaultRetention.Years == 0) {
			return fmt.Errorf("Either Days or Years must be set, not both")
		}
	}
	return nil
}

func (s *objStoreServer) getObjectMutex(objectKey string) *sync.Mutex {
	s.objectMutexLock.Lock()
	defer s.objectMutexLock.Unlock()

	if _, exists := s.objectMutexes[objectKey]; !exists {
		s.objectMutexes[objectKey] = &sync.Mutex{}
	}
	return s.objectMutexes[objectKey]
}

func (s *objStoreServer) handleObjectOperation(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName := vars["bucket"]
	objectKey := vars["key"]

	s.objectLockMutex.RLock()
	lockConfig, exists := s.objectLocks[bucketName]
	s.objectLockMutex.RUnlock()

	if exists && lockConfig.ObjectLockEnabled == "Enabled" {
		// Check if the object is locked
		if s.isObjectLocked(bucketName, objectKey) {
			http.Error(w, "Object is locked", http.StatusForbidden)
			return
		}
	}

	// Get the mutex for this object
	objectMutex := s.getObjectMutex(objectKey)
	objectMutex.Lock()
	defer objectMutex.Unlock()

	// Perform the object operation (e.g., copy, delete)
	s.performObjectOperation(w, r, bucketName, objectKey)
}

func (s *objStoreServer) isObjectLocked(bucketName, objectKey string) bool {
	// Implement logic to check if the object is locked
	// This could involve checking a database or in-memory store for the object's lock status
	return false // Placeholder
}

func (s *objStoreServer) performObjectOperation(w http.ResponseWriter, r *http.Request, bucketName, objectKey string) {
	// Implement the actual object operation (e.g., copy, delete)
	// For example, for a copy operation:
	if r.Method == http.MethodPut {
		destPath := fmt.Sprintf("/tmp/%s-%s", bucketName, objectKey)
		destFile, err := os.Create(destPath)
		if err != nil {
			http.Error(w, "Failed to create destination file", http.StatusInternalServerError)
			return
		}
		defer destFile.Close()

		_, err = io.Copy(destFile, r.Body)
		if err != nil {
			http.Error(w, "Failed to copy object", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Object copied successfully")
	} else {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
	}
}

func printHTTPHeader(r *http.Request) {
	fmt.Println("Headers received from client:")
	for key, values := range r.Header {
		for _, value := range values {
			fmt.Printf("%s: %s\n", key, value)
		}
	}
}

// EnableVersioning enables versioning for a bucket
func (s *objStoreServer) EnableVersioning(bucketName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.BucketExists(bucketName) {
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}

	// Implement versioning logic here
	// For simplicity, we'll just set a flag in the bucket metadata
	bucketPath := filepath.Join(s.dataDir, bucketName)
	versioningFlag := filepath.Join(bucketPath, ".versioning_enabled")
	return ioutil.WriteFile(versioningFlag, []byte("true"), 0644)
}

// DisableVersioning disables versioning for a bucket
func (s *objStoreServer) DisableVersioning(bucketName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.BucketExists(bucketName) {
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}

	bucketPath := filepath.Join(s.dataDir, bucketName)
	versioningFlag := filepath.Join(bucketPath, ".versioning_enabled")
	return os.Remove(versioningFlag)
}

// CopyObject copies an object from one location to another
func (s *objStoreServer) CopyObject(srcBucket, srcObject, dstBucket, dstObject string) error {
	srcPath := filepath.Join(s.dataDir, srcBucket, srcObject)
	dstPath := filepath.Join(s.dataDir, dstBucket, dstObject)

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// StatObject retrieves metadata about an object
func (s *objStoreServer) StatObject(bucketName, objectName string) (*ObjectInfo, error) {
	objectPath := filepath.Join(s.dataDir, bucketName, objectName)
	info, err := os.Stat(objectPath)
	if err != nil {
		return nil, err
	}

	return &ObjectInfo{
		Key:          objectName,
		Size:         info.Size(),
		LastModified: info.ModTime(),
		ETag:         fmt.Sprintf("\"%x\"", md5.Sum([]byte(objectPath))),
		//contentType:  r.Header.Get("Content-Type"),
	}, nil
}

// RemoveObjects removes multiple objects from a bucket
func (s *objStoreServer) RemoveObjects(bucketName string, objectNames []string) error {
	for _, objectName := range objectNames {
		err := s.DeleteObject(bucketName, objectName)
		if err != nil {
			return err
		}
	}
	return nil
}

// PresignedGetObject generates a presigned URL for GET operation
func (s *objStoreServer) PresignedGetObject(bucketName, objectName string, expiry time.Duration) (string, error) {
	if !s.BucketExists(bucketName) {
		return "", fmt.Errorf("bucket %s does not exist", bucketName)
	}

	objectPath := filepath.Join(s.dataDir, bucketName, objectName)
	if _, err := os.Stat(objectPath); os.IsNotExist(err) {
		return "", fmt.Errorf("object %s does not exist", objectName)
	}

	// Generate a unique token
	token := uuid.New().String()

	// Create a signed URL
	u, err := url.Parse(fmt.Sprintf("http://localhost:8080/%s/%s", bucketName, objectName))
	if err != nil {
		return "", err
	}

	q := u.Query()
	q.Set("token", token)
	q.Set("expires", strconv.FormatInt(time.Now().Add(expiry).Unix(), 10))
	u.RawQuery = q.Encode()

	// Store the token in memory (in a production environment, use a more persistent storage)
	// Implementation of token storage is omitted for brevity

	return u.String(), nil
}

// SetBucketPolicy sets the policy for a bucket
func (s *objStoreServer) SetBucketPolicy(bucketName, policy string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.BucketExists(bucketName) {
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}

	policyPath := filepath.Join(s.dataDir, bucketName, ".bucket_policy")
	return ioutil.WriteFile(policyPath, []byte(policy), 0644)
}

// GetBucketPolicy retrieves the policy for a bucket
func (s *objStoreServer) GetBucketPolicy(bucketName string) (string, error) {
	if !s.BucketExists(bucketName) {
		return "", fmt.Errorf("bucket %s does not exist", bucketName)
	}

	policyPath := filepath.Join(s.dataDir, bucketName, ".bucket_policy")
	policy, err := ioutil.ReadFile(policyPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil // No policy set
		}
		return "", err
	}

	return string(policy), nil
}

// SetAppInfo sets custom application information
func (s *objStoreServer) SetAppInfo(appName, appVersion string) {
	// Store app info in the server struct
	/* s.appInfo = struct {
		Name    string
		Version string
	}{
		Name:    appName,
		Version: appVersion,
	}
	*/
}

// TraceOn enables request tracing
func (s *objStoreServer) TraceOn() {
	//s.traceEnabled = true
}

// TraceOff disables request tracing
func (s *objStoreServer) TraceOff() {
	//s.traceEnabled = false
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
func (s *objStoreServer) deleteBucket(bucket string) error {
	bucketPath := filepath.Join(s.dataDir, bucket)
	err := s.DeleteBucket(bucketPath)
	if err == nil {
		s.mu.Lock()
		replicaBucket, exists := s.bucketReplication[bucket]
		s.mu.Unlock()
		if exists {
			bucketPath = filepath.Join(s.dataDirReplica, replicaBucket)
			err = s.DeleteBucket(bucketPath)
		}
	}
	s.mu.Lock()
	delete(s.bucketNames, bucket)
	s.mu.Unlock()
	return err
}
func (s *objStoreServer) createDirectoriesAll(bucket, objectKey string) error {
	fullPath := filepath.Join(s.dataDir, bucket, objectKey)
	err := os.MkdirAll(fullPath, 0755)
	if err != nil {
		return err
	}

	s.mu.Lock()
	replicaBucket, exists := s.bucketReplication[bucket]
	s.mu.Unlock()
	if exists {
		fullPath = filepath.Join(s.dataDirReplica, replicaBucket, objectKey)
		err = os.MkdirAll(fullPath, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}
