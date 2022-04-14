package weed

import (
	"context"

	"github.com/minio/madmin-go"
	minio "github.com/minio/minio/cmd"
)

func (w *weedObjects) LocalStorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	return w.StorageInfo(ctx)
}

func (w *weedObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	return minio.StorageInfo{
		Disks:   []madmin.Disk{},
		Backend: madmin.BackendInfo{},
	}, nil
}

func (w *weedObjects) BackendDataUsageInfo(ctx context.Context, objAPI minio.ObjectLayer) (minio.DataUsageInfo, error) {

	return minio.DataUsageInfo{}, nil
}
