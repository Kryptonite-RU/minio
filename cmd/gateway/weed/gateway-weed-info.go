package weed

import (
	"context"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/super_block"
	"github.com/minio/madmin-go"
	minio "github.com/minio/minio/cmd"
)

type CollectionInfo struct {
	FileCount        float64
	DeleteCount      float64
	DeletedByteCount float64
	Size             float64
	VolumeCount      int
}

func (w *weedObjects) LocalStorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	return w.StorageInfo(ctx)
}

func (w *weedObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	return minio.StorageInfo{
		Disks:   []madmin.Disk{},
		Backend: madmin.BackendInfo{},
	}, nil
}

func (w *weedObjects) listCollectionNames() (collections []string, err error) {
	var resp *master_pb.CollectionListResponse
	err = pb.WithMasterClient(false, w.Client.option.MasterClient.GetMaster(), w.Client.option.GrpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, err = client.CollectionList(context.Background(), &master_pb.CollectionListRequest{
			IncludeNormalVolumes: true,
			IncludeEcVolumes:     true,
		})
		return err
	})
	if err != nil {
		return
	}
	for _, c := range resp.Collections {
		if len(c.Name) > 0 {
			collections = append(collections, c.Name)
		}
	}
	return
}

func (w *weedObjects) collectTopologyInfo() (topoInfo *master_pb.TopologyInfo, volumeSizeLimitMb uint64, err error) {
	var resp *master_pb.VolumeListResponse
	err = pb.WithMasterClient(false, w.Client.option.MasterClient.GetMaster(), w.Client.option.GrpcDialOption, func(client master_pb.SeaweedClient) error {
		resp, err = client.VolumeList(context.Background(), &master_pb.VolumeListRequest{})
		return err
	})
	if err != nil {
		return
	}

	return resp.TopologyInfo, resp.VolumeSizeLimitMb, nil

}

func (w *weedObjects) BackendDataUsageInfo(ctx context.Context, objAPI minio.ObjectLayer) (minio.DataUsageInfo, error) {

	collections, err := w.listCollectionNames()
	if err != nil {
		return minio.DataUsageInfo{}, err
	}

	topologyInfo, _, err := w.collectTopologyInfo()
	if err != nil {
		return minio.DataUsageInfo{}, err
	}

	collectionInfos := make(map[string]*CollectionInfo)

	collectCollectionInfo(topologyInfo, collectionInfos)

	var dataUsageInfo minio.DataUsageInfo
	dataUsageInfo.BucketsUsage = make(map[string]minio.BucketUsageInfo)
	dataUsageInfo.BucketSizes = make(map[string]uint64)
	for _, c := range collections {
		cif, found := collectionInfos[c]
		if !found {
			continue
		}
		size := uint64(cif.Size)
		count := uint64(cif.FileCount)
		dataUsageInfo.BucketsCount += 1
		dataUsageInfo.ObjectsTotalSize += size
		dataUsageInfo.ObjectsTotalCount += count
		dataUsageInfo.BucketsUsage[c] = minio.BucketUsageInfo{Size: size, ObjectsCount: count}
		dataUsageInfo.BucketSizes[c] = size

	}

	dataUsageInfo.LastUpdate = time.Now()
	return dataUsageInfo, nil
}

func addToCollection(collectionInfos map[string]*CollectionInfo, vif *master_pb.VolumeInformationMessage) {
	c := vif.Collection
	cif, found := collectionInfos[c]
	if !found {
		cif = &CollectionInfo{}
		collectionInfos[c] = cif
	}
	replicaPlacement, _ := super_block.NewReplicaPlacementFromByte(byte(vif.ReplicaPlacement))
	copyCount := float64(replicaPlacement.GetCopyCount())
	cif.Size += float64(vif.Size) / copyCount
	cif.DeleteCount += float64(vif.DeleteCount) / copyCount
	cif.FileCount += float64(vif.FileCount) / copyCount
	cif.DeletedByteCount += float64(vif.DeletedByteCount) / copyCount
	cif.VolumeCount++
}

func collectCollectionInfo(t *master_pb.TopologyInfo, collectionInfos map[string]*CollectionInfo) {
	for _, dc := range t.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, diskInfo := range dn.DiskInfos {
					for _, vi := range diskInfo.VolumeInfos {
						addToCollection(collectionInfos, vi)
					}
				}
			}
		}
	}
}
