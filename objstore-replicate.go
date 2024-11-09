package main

import (
	"encoding/json"
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

	"github.com/fsnotify/fsnotify"
	"github.com/gorilla/mux"
)

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

func (s *objStoreServer) handlePutBucketReplication(w http.ResponseWriter, r *http.Request) {
	bucketName := mux.Vars(r)["bucket"]

	// 1. Check bucket exists
	s.mu.Lock()
	if _, exists := s.bucketNames[bucketName]; !exists {
		s.sendErrorResponse(w, "NoSuchBucket", http.StatusNotFound)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	switch r.Method {
	case "GET":
		s.handleGetBucketReplication(w, r, bucketName)

	case "PUT":
		// 2. Parse Replication configuration file
		replicationConfig, err := parseReplicationConfig(r.Body)
		if err != nil {
			s.sendErrorResponse(w, "InvalidReplicationConfigurationXML", http.StatusBadRequest)
			return
		}

		// 3. Get the replication config data
		replicationConfigJSON, err := json.Marshal(replicationConfig)
		if err != nil {
			s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
			return
		}

		// 4. Get the replication bucket name
		bucketReplName, err := getBucketReplicateName(string(replicationConfigJSON))
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}

		// 5. Store the replication bucket name
		s.mu.Lock()
		s.bucketReplication[bucketName] = bucketReplName
		s.bucketNames[bucketName] = true
		s.bucketReplicationConfig[bucketName] = replicationConfig
		s.mu.Unlock()

		// 6. Create replica bucket
		s.CreateBucketReplica(bucketReplName)

		// 7. Send success response
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Replication configuration applied successfully"))
	case "DELETE":
		s.mu.Lock()
		bucketReplName := s.bucketReplication[bucketName]
		s.mu.Unlock()

		s.DeleteBucketReplica(bucketReplName)

		s.mu.Lock()
		delete(s.bucketReplication, bucketName)
		delete(s.bucketNames, bucketName)
		delete(s.bucketReplicationConfig, bucketName)
		s.mu.Unlock()
	}
}

type ReplicationConfig struct {
	Role  string `json:"Role"`
	Rules []struct {
		Status                  string `json:"Status"`
		Priority                int    `json:"Priority"`
		DeleteMarkerReplication struct {
			Status string `json:"Status"`
		} `json:"DeleteMarkerReplication"`
		Filter struct {
			Prefix string `json:"Prefix"`
		} `json:"Filter"`
		Destination struct {
			Bucket string `json:"Bucket"`
		} `json:"Destination"`
	} `json:"Rules"`
}

type XMLReplicationConfiguration struct {
	XMLName xml.Name `xml:"ReplicationConfiguration"`
	Role    string   `xml:"Role"`
	Rules   []struct {
		Status                  string `xml:"Status"`
		Priority                int    `xml:"Priority"`
		DeleteMarkerReplication struct {
			Status string `xml:"Status"`
		} `xml:"DeleteMarkerReplication"`
		Filter struct {
			Prefix string `xml:"Prefix"`
		} `xml:"Filter"`
		Destination struct {
			Bucket string `xml:"Bucket"`
		} `xml:"Destination"`
	} `xml:"Rule"`
}

func parseReplicationConfig(body io.Reader) (ReplicationConfig, error) {
	// Read the entire body
	bodyBytes, err := ioutil.ReadAll(body)
	if err != nil {
		return ReplicationConfig{}, fmt.Errorf("failed to read request body: %v", err)
	}

	var xmlConfig XMLReplicationConfiguration
	err = xml.Unmarshal(bodyBytes, &xmlConfig)
	if err != nil {
		return ReplicationConfig{}, fmt.Errorf("failed to parse XML: %v", err)
	}

	// Convert XML config to ReplicationConfig
	config := ReplicationConfig{
		Role: xmlConfig.Role,
		Rules: make([]struct {
			Status                  string `json:"Status"`
			Priority                int    `json:"Priority"`
			DeleteMarkerReplication struct {
				Status string `json:"Status"`
			} `json:"DeleteMarkerReplication"`
			Filter struct {
				Prefix string `json:"Prefix"`
			} `json:"Filter"`
			Destination struct {
				Bucket string `json:"Bucket"`
			} `json:"Destination"`
		}, len(xmlConfig.Rules)),
	}

	for i, rule := range xmlConfig.Rules {
		config.Rules[i] = struct {
			Status                  string `json:"Status"`
			Priority                int    `json:"Priority"`
			DeleteMarkerReplication struct {
				Status string `json:"Status"`
			} `json:"DeleteMarkerReplication"`
			Filter struct {
				Prefix string `json:"Prefix"`
			} `json:"Filter"`
			Destination struct {
				Bucket string `json:"Bucket"`
			} `json:"Destination"`
		}{
			Status:   rule.Status,
			Priority: rule.Priority,
			DeleteMarkerReplication: struct {
				Status string `json:"Status"`
			}{Status: rule.DeleteMarkerReplication.Status},
			Filter: struct {
				Prefix string `json:"Prefix"`
			}{Prefix: rule.Filter.Prefix},
			Destination: struct {
				Bucket string `json:"Bucket"`
			}{Bucket: rule.Destination.Bucket},
		}
	}

	// Validate the parsed configuration
	if config.Role == "" {
		return ReplicationConfig{}, fmt.Errorf("Role is required in replication configuration")
	}
	if len(config.Rules) == 0 {
		return ReplicationConfig{}, fmt.Errorf("At least one rule is required in replication configuration")
	}
	for i, rule := range config.Rules {
		if rule.Status != "Enabled" && rule.Status != "Disabled" {
			return ReplicationConfig{}, fmt.Errorf("Invalid Status for rule %d: must be 'Enabled' or 'Disabled'", i)
		}
		if rule.Destination.Bucket == "" {
			return ReplicationConfig{}, fmt.Errorf("Destination Bucket is required for rule %d", i)
		}
	}
	return config, nil
}

func getBucketReplicateName(jsonStr string) (string, error) {
	var config struct {
		Rules []struct {
			Destination struct {
				Bucket string `json:"Bucket"`
			} `json:"Destination"`
		} `json:"Rules"`
	}

	err := json.Unmarshal([]byte(jsonStr), &config)
	if err != nil {
		return "", fmt.Errorf("failed to parse JSON: %v", err)
	}

	if len(config.Rules) == 0 {
		return "", fmt.Errorf("no replication rules found")
	}

	bucketARN := config.Rules[0].Destination.Bucket
	parts := strings.Split(bucketARN, ":")
	if len(parts) < 6 {
		return "", fmt.Errorf("invalid bucket ARN format")
	}

	bucketName := parts[5]
	return bucketName, nil
}

func (s *objStoreServer) handleGetBucketReplication(w http.ResponseWriter, r *http.Request, bucketName string) {
	s.mu.Lock()
	replicationConfig, exists := s.bucketReplicationConfig[bucketName]
	s.mu.Unlock()

	if !exists {
		s.sendErrorResponse(w, "ReplicationConfigurationNotFoundError", http.StatusNotFound)
		return
	}

	// Convert the replication configuration to XML
	xmlConfig := XMLReplicationConfiguration{
		Role: replicationConfig.Role,
		Rules: make([]struct {
			Status                  string `xml:"Status"`
			Priority                int    `xml:"Priority"`
			DeleteMarkerReplication struct {
				Status string `xml:"Status"`
			} `xml:"DeleteMarkerReplication"`
			Filter struct {
				Prefix string `xml:"Prefix"`
			} `xml:"Filter"`
			Destination struct {
				Bucket string `xml:"Bucket"`
			} `xml:"Destination"`
		}, len(replicationConfig.Rules)),
	}

	for i, rule := range replicationConfig.Rules {
		xmlConfig.Rules[i] = struct {
			Status                  string `xml:"Status"`
			Priority                int    `xml:"Priority"`
			DeleteMarkerReplication struct {
				Status string `xml:"Status"`
			} `xml:"DeleteMarkerReplication"`
			Filter struct {
				Prefix string `xml:"Prefix"`
			} `xml:"Filter"`
			Destination struct {
				Bucket string `xml:"Bucket"`
			} `xml:"Destination"`
		}{
			Status:   rule.Status,
			Priority: rule.Priority,
			DeleteMarkerReplication: struct {
				Status string `xml:"Status"`
			}{Status: rule.DeleteMarkerReplication.Status},
			Filter: struct {
				Prefix string `xml:"Prefix"`
			}{Prefix: rule.Filter.Prefix},
			Destination: struct {
				Bucket string `xml:"Bucket"`
			}{Bucket: rule.Destination.Bucket},
		}
	}

	// Marshal the XML configuration
	output, err := xml.MarshalIndent(xmlConfig, "", "  ")
	if err != nil {
		s.sendErrorResponse(w, "InternalError", http.StatusInternalServerError)
		return
	}

	// Set the content type to XML and write the response
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(xml.Header))
	w.Write(output)
}

func NewSyncManager(syncInterval time.Duration) (*SyncManager, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &SyncManager{
		watcher:       watcher,
		watchedPaths:  make(map[string]bool),
		syncInterval:  syncInterval,
		lastSyncTimes: make(map[string]time.Time),
	}, nil
}

func (sm *SyncManager) AddPath(path string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.watchedPaths[path] {
		return nil // Already watching this path
	}

	err := sm.watcher.Add(path)
	if err != nil {
		return err
	}

	sm.watchedPaths[path] = true
	sm.lastSyncTimes[path] = time.Now()

	go sm.monitorPath(path)
	return nil
}

func (sm *SyncManager) monitorPath(path string) {
	for {
		select {
		case event, ok := <-sm.watcher.Events:
			if !ok {
				return
			}
			if event.Has(fsnotify.Write) || event.Has(fsnotify.Create) || event.Has(fsnotify.Remove) {
				sm.handleFileChange(event.Name)
			}
		case err, ok := <-sm.watcher.Errors:
			if !ok {
				return
			}
			log.Println("error:", err)
		}
	}
}

func (sm *SyncManager) handleFileChange(path string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	lastSync, exists := sm.lastSyncTimes[path]
	if !exists || time.Since(lastSync) > sm.syncInterval {
		sm.syncFile(path)
		sm.lastSyncTimes[path] = time.Now()
	}
}

func (sm *SyncManager) syncFile(path string) {
	// This could involve uploading the file to S3, updating metadata, etc.
	fmt.Printf("Syncing file: %s\n", path)
	// Example: uploadToS3(path)
}
