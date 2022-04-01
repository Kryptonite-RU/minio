package weed

import minio "github.com/minio/minio/cmd"

const (
	BucketDir     = "/buckets"
	weedSeparator = minio.SlashSeparator
	defaultEtag   = "00000000000000000000000000000000-1"

	multipartUploadDir = ".uploads"
)
