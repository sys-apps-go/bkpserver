package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	bucketName      = "test-bucket"
	endpoint        = "http://localhost:50051" // Replace with your server's endpoint
	accessKeyID     = "objstoreadmin"
	secretAccessKey = "objstoreadmin"
	region          = "us-east-1"
)

func main() {
	// Create a custom resolver that always returns your endpoint
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"), // Replace with appropriate region
		Endpoint:         aws.String(endpoint),    // Specify your custom endpoint (MinIO or AWS S3)
		S3ForcePathStyle: aws.Bool(true),          // Enable for path-style access
		Credentials:      credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
	})
	if err != nil {
		return
	}

	// Create an S3 client
	client := s3.New(sess)

	// Create bucket if it doesn't exist
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

	// List buckets
	result, err := client.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		log.Printf("Couldn't list buckets. Here's why: %v\n", err)
	} else {
		fmt.Println("Buckets:")
		for _, bucket := range result.Buckets {
			fmt.Printf("\t%s\n", *bucket.Name)
		}
	}

	// List objects in the bucket
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

	// Delete the bucket
	_, err = client.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		log.Printf("Couldn't delete bucket %v. Here's why: %v\n", bucketName, err)
	} else {
		fmt.Printf("Deleted bucket %v\n", bucketName)
	}
}
