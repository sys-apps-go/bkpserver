package main

import (
	"fmt"
	"log"
	"io/ioutil"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	bucketName         = "test-bucket"
	endpoint           = "http://localhost:50051" // Replace with your server's endpoint
	accessKeyID        = "objstoreadmin"
	secretAccessKey    = "objstoreadmin"
	region             = "us-east-1"
	configFile         = "/tmp/replication.json"
	multipartThreshold = 5 * 1024 * 1024 // 5 MB
	sourceDir          = "/test1/tmp/minio"
)

func main() {
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String(region),
		Endpoint:         aws.String(endpoint),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
	})
	if err != nil {
		log.Fatalf("Failed to create session: %v", err)
	}

	// 1. Create an S3 client
	client := s3.New(sess)

	// 2. Create bucket if it doesn't exist
	_, err = client.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region),
		},
	})

	if err != nil {
		log.Printf("Couldn't create bucket %v. Here's why: %v\n", bucketName, err)
	} else {
		fmt.Printf("Created bucket %v\n", bucketName)
	}

	// 3. List buckets
	result, err := client.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		log.Printf("Couldn't list buckets. Here's why: %v\n", err)
	} else {
		fmt.Println("Buckets:")
		for _, bucket := range result.Buckets {
			fmt.Printf("\t%s\n", *bucket.Name)
		}
	}

	// 4. List objects in the bucket
	listObjsResponse, err := client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
	})

	if err != nil {
		log.Printf("Couldn't list objects in bucket %v. Here's why: %v\n", bucketName, err)
	} else {
		fmt.Printf("Objects in bucket %v:\n", bucketName)
		for _, object := range listObjsResponse.Contents {
			fmt.Printf("\t%s\n", *object.Key)
		}
	}

	// Read the configuration file
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	// Parse the JSON configuration
	var replicationConfig s3.ReplicationConfiguration
	err = json.Unmarshal(configData, &replicationConfig)
	if err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}

	// Put the replication configuration
	input := &s3.PutBucketReplicationInput{
		Bucket:                   aws.String(bucketName),
		ReplicationConfiguration: &replicationConfig,
	}
	_, err = client.PutBucketReplication(input)
	if err != nil {
		log.Fatalf("Failed to put bucket replication: %v", err)
	}
	fmt.Println("Successfully set bucket replication configuration")

	inputGet := &s3.GetBucketReplicationInput{
		Bucket: aws.String(bucketName),
	}
	resultGet, err := client.GetBucketReplication(inputGet)
	if err != nil {
		log.Fatalf("Failed to get bucket replication: %v", err)
	}
	configJSON, err := json.MarshalIndent(resultGet.ReplicationConfiguration, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal replication config: %v", err)
	}
	fmt.Printf("Replication configuration for bucket %s:\n%s\n", bucketName, string(configJSON))

	inputDelete := &s3.DeleteBucketReplicationInput{
		Bucket: aws.String(bucketName),
	}

	_, err = client.DeleteBucketReplication(inputDelete)
	if err != nil {
		log.Fatalf("Failed to delete bucket replication: %v", err)
	}

	// Create uploader
	uploader := s3manager.NewUploader(sess)

	// Walk through the source directory
	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if info.IsDir() {
			return nil
		}

		// Create the S3 key (object name)
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}
		s3Key := filepath.ToSlash(relPath)

		// Open the file
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		// Upload the file
		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
			Body:   file,
		})

		if err != nil {
			fmt.Printf("Failed to upload %s: %v\n", path, err)
			return err
		}

		fmt.Printf("Uploaded: %s\n", s3Key)
		return nil
	})

	if err != nil {
		fmt.Printf("Error walking the path %v: %v\n", sourceDir, err)
	}

	// Delete the bucket
	_, err = client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Printf("Couldn't delete bucket %v. Here's why: %v\n", bucketName, err)
	} else {
		fmt.Printf("Deleted bucket %v\n", bucketName)
	}
	fmt.Println("Successfully deleted bucket replication configuration")
}

