package weed

import minio "github.com/minio/minio/cmd"

const (
	BucketDir     = "/buckets"
	weedSeparator = minio.SlashSeparator

	minioMetaBucket     = ".minio.sys"
	minioMetaTmpDir     = "tmp"
	minioMetaTmpBucket  = minioMetaBucket + "/" + minioMetaTmpDir
	minioReservedBucket = "minio"

	multipartUploadDir = BucketDir + "/" + minioMetaTmpBucket
)
