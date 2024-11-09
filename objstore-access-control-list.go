package main

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
)

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

func (s *objStoreServer) getBucketACL(ctx context.Context, bucketName string) (*BucketACL, error) {
	// This is a placeholder implementation
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
	// For example:

	// 1. Validate the ACL
	if err := s.validateObjectACL(acl); err != nil {
		return err
	}

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
