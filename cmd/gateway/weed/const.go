package weed

import minio "github.com/minio/minio/cmd"

const (
	BucketDir     = "/buckets"
	weedSeparator = minio.SlashSeparator

	minioMetaBucket     = ".minio.sys"
	minioMetaTmpBucket  = minioMetaBucket + "/tmp"
	minioReservedBucket = "minio"
)
