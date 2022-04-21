// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio/internal/logger"
)

const (
	dataUsageRoot   = SlashSeparator
	dataUsageBucket = minioMetaBucket + SlashSeparator + bucketMetaPrefix

	dataUsageObjName       = ".usage.json"
	dataUsageObjNamePath   = bucketMetaPrefix + SlashSeparator + dataUsageObjName
	dataUsageBloomName     = ".bloomcycle.bin"
	dataUsageBloomNamePath = bucketMetaPrefix + SlashSeparator + dataUsageBloomName

	dataUsageCacheName = ".usage-cache.bin"
)

// storeDataUsageInBackend will store all objects sent on the gui channel until closed.
func storeDataUsageInBackend(ctx context.Context, objAPI ObjectLayer, dui <-chan DataUsageInfo) {
	for dataUsageInfo := range dui {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		dataUsageJSON, err := json.Marshal(dataUsageInfo)
		if err != nil {
			logger.LogIf(ctx, err)
			continue
		}
		if err = saveConfig(ctx, objAPI, dataUsageObjNamePath, dataUsageJSON); err != nil {
			logger.LogIf(ctx, err)
		}
	}
}

// loadPrefixUsageFromBackend returns prefix usages found in passed buckets
//   e.g.:  /testbucket/prefix => 355601334
func loadPrefixUsageFromBackend(ctx context.Context, objAPI ObjectLayer, bucket string) (map[string]uint64, error) {
	z, ok := objAPI.(*erasureServerPools)
	if !ok {
		// Prefix usage is empty
		return map[string]uint64{}, nil
	}

	cache := dataUsageCache{}

	m := make(map[string]uint64)
	for _, pool := range z.serverPools {
		for _, er := range pool.sets {
			// Load bucket usage prefixes
			if err := cache.load(ctx, er, bucket+slashSeparator+dataUsageCacheName); err == nil {
				root := cache.find(bucket)
				if root == nil {
					// We dont have usage information for this bucket in this
					// set, go to the next set
					continue
				}

				for id, usageInfo := range cache.flattenChildrens(*root) {
					prefix := decodeDirObject(strings.TrimPrefix(id, bucket+slashSeparator))
					// decodeDirObject to avoid any __XL_DIR__ objects
					m[prefix] += uint64(usageInfo.Size)
				}
			}
		}
	}

	return m, nil
}

func loadDataUsageFromBackend(ctx context.Context, objAPI ObjectLayer) (DataUsageInfo, error) {
	objLayer := newObjectLayerFn()
	if objLayer != nil && (!globalIsGateway || globalGatewayName == WeedBackendGateway) {
		dataUsageInfo, err := objLayer.BackendDataUsageInfo(ctx, objAPI)
		if err != nil {
			return DataUsageInfo{}, err
		}
		return dataUsageInfo, nil
	}

	// return empty if backend not supported DataUsageInfo
	return DataUsageInfo{}, nil
}
