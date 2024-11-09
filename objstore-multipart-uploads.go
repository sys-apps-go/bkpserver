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
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)


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
	err := s.createDirectoriesAndControlFiles(s.dataDir, bucket, key, metadata)
	if err != nil {
		return err
	}

	fullPath := filepath.Join(s.dataDir, bucket, key)
	// Write the file
	if err := ioutil.WriteFile(fullPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}
	s.mu.Lock()
	replicaBucket, exists := s.bucketReplication[bucket]
	s.mu.Unlock()
	if exists {
		err = s.createDirectoriesAndControlFiles(s.dataDirReplica, replicaBucket, key, metadata)
		if err != nil {
			return err
		}
		fullPath = filepath.Join(s.dataDirReplica, replicaBucket, key)
		// Write the file
		if err := ioutil.WriteFile(fullPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write file: %v", err)
		}
	}

	return nil
}

func (s *objStoreServer) createDirectoriesAndControlFiles(dataDir, bucket, objectKey string, metadataReq map[string]string) error {
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
		for key, value := range metadataReq {
			metadata[key] = value
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

	err := s.CompleteMultipartUpload(bucketName, objectKey, uploadID, completeMultipartUpload.Parts, r)
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

func (s *objStoreServer) CompleteMultipartUpload(bucket, key, uploadID string, parts []CompletedPart, r *http.Request) error {
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
		"CreatedAt": time.Now().UTC().Format(time.RFC3339),
		"Path":      controlFilePath,
		"UploadID":  uploadID,
		"Parts":     fmt.Sprintf("%d", len(parts)),
	}

	m := extractMetadata(r.Header)
	for key, value := range m {
		metadata[key] = value
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

	s.mu.Lock()
	_, exists := s.bucketReplication[bucket]
	s.mu.Unlock()
	if exists {
		// Replica the object to the mirror bucket
		err = s.replicateObjectFileCopy(bucket, key, finalPath, controlFilePath)
		if err != nil {
			log.Printf("Failed to replicate object %s/%s: %v", bucket, key, err)
			// Optionally, you could return this error if you want to ensure replication succeeds
			// return fmt.Errorf("failed to replicate object: %v", err)
		}
	}

	return nil
}

func (s *objStoreServer) replicateObjectFileCopy(bucketName, key string, fullPath, controlFileFullPath string) error {
	sourcePath := fullPath
	replicaPath := filepath.Join(s.dataDirReplica, bucketName, key)
	replicaControlFileFullPath := filepath.Join(filepath.Dir(replicaPath), filepath.Base(controlFileFullPath))

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

	sourceFile, err = os.Open(controlFileFullPath)
	if err != nil {
		return fmt.Errorf("failed to open control file: %v", err)
	}
	defer sourceFile.Close()

	// Create the replica file
	replicaFile, err = os.Create(replicaControlFileFullPath)
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
