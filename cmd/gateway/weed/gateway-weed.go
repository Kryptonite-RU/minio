package weed

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/minio/cli"
	"github.com/minio/madmin-go"
	"github.com/minio/minio-go/v6/pkg/s3utils"
	minio "github.com/minio/minio/cmd"
	"google.golang.org/grpc"
)

func init() {
	const weedGatewayTemplate = `NAME:
  {{.HelpName}} - {{.Usage}}

USAGE:
  {{.HelpName}} {{if .VisibleFlags}}[FLAGS]{{end}} [WEED-MASTER]
{{if .VisibleFlags}}
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}{{end}}
WEED-MASTER:
  seaweedfs server master uri.

EXAMPLES:
  1. Start minio gateway server for SEAWEEDFS backend
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.HelpName}}

  2. Start minio gateway server for SEAWEEDFS backend with edge caching enabled
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_USER{{.AssignmentOperator}}accesskey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_ROOT_PASSWORD{{.AssignmentOperator}}secretkey
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_DRIVES{{.AssignmentOperator}}"/mnt/drive1,/mnt/drive2,/mnt/drive3,/mnt/drive4"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_EXCLUDE{{.AssignmentOperator}}"bucket1/*,*.png"
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_QUOTA{{.AssignmentOperator}}90
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_AFTER{{.AssignmentOperator}}3
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_LOW{{.AssignmentOperator}}75
     {{.Prompt}} {{.EnvVarSetCommand}} MINIO_CACHE_WATERMARK_HIGH{{.AssignmentOperator}}85
     {{.Prompt}} {{.HelpName}}
`

	minio.RegisterGatewayCommand(cli.Command{
		Name:               minio.WeedBackendGateway,
		Usage:              "Seaweedfs distributed file system",
		Action:             WeedGatewayMain,
		CustomHelpTemplate: weedGatewayTemplate,
		HideHelpCommand:    true,
	})
}

func WeedGatewayMain(ctx *cli.Context) {
	args := ctx.Args()
	if !ctx.Args().Present() {
		args = cli.Args{"127.0.0.1:8888"}
	}

	minio.StartGateway(ctx, &Weed{filer: args.First()})
}

type Weed struct {
	filer string
}

func (w *Weed) Name() string {
	return minio.WeedBackendGateway
}

func (w *Weed) NewGatewayLayer(creds madmin.Credentials) (minio.ObjectLayer, error) {
	metrics := minio.NewMetrics()

	t := &minio.MetricsTransport{
		Transport: minio.NewGatewayHTTPTransport(),
		Metrics:   metrics,
	}

	weedOptions := &WeedOptions{
		Filer:          pb.ServerAddress(w.filer),
		GrpcDialOption: grpc.WithInsecure(),
	}

	clnt, err := NewWeedClient(weedOptions)

	if err != nil {
		return nil, err
	}

	weedGateway := weedObjects{
		Client: clnt,
		HTTPClient: &http.Client{
			Transport: t,
		},
		Metrics:  metrics,
		listPool: minio.NewTreeWalkPool(time.Minute * 30),
	}
	return &weedGateway, nil
}

type weedObjects struct {
	minio.GatewayUnsupported
	Client     *WeedClient
	HTTPClient *http.Client
	Metrics    *minio.BackendMetrics
	listPool   *minio.TreeWalkPool
}

func (w *weedObjects) weedPathJoin(args ...string) string {
	return minio.PathJoin(append([]string{BucketDir, weedSeparator}, args...)...)
}

func (w *weedObjects) getObject(ctx context.Context, bucket, key string, startOffset, length int64, writer io.Writer, etag string, opts minio.ObjectOptions) error {
	path := util.NewFullPath(w.weedPathJoin(bucket), key)
	destURL := fmt.Sprintf("http://%s%s", w.Client.option.Filer, path)
	resp, err := http.Get(destURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	rd := bytes.NewReader(bodyBytes)
	_, err = io.Copy(writer, io.NewSectionReader(rd, startOffset, length))
	if err == io.ErrClosedPipe {
		err = nil
	}
	return nil
}

func (w *weedObjects) GetObjectInfo(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	path := util.NewFullPath(w.weedPathJoin(bucket), object)

	if strings.HasSuffix(object, weedSeparator) && !w.isObjectDir(ctx, bucket, object) {
		return objInfo, minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	entry, err := filer_pb.GetEntry(w.Client, path)
	if err == filer_pb.ErrNotFound {
		return objInfo, minio.ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return objInfo, err
	}

	if !strings.HasSuffix(object, weedSeparator) && entry.IsDirectory {
		return objInfo, minio.ObjectNotFound{Bucket: bucket, Object: object}
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ModTime: time.Unix(entry.Attributes.Mtime, 0),
		Size:    int64(entry.Attributes.FileSize),
		IsDir:   entry.IsDirectory,
		AccTime: time.Time{},
	}, nil
}

func (w *weedObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *minio.HTTPRangeSpec, h http.Header, lockType minio.LockType, opts minio.ObjectOptions) (gr *minio.GetObjectReader, err error) {
	var objInfo minio.ObjectInfo
	objInfo, err = w.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}
	var startOffset, length int64
	startOffset, length, err = rs.GetOffsetLength(objInfo.Size)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	go func() {
		nerr := w.getObject(ctx, bucket, object, startOffset, length, pw, objInfo.ETag, opts)
		pw.CloseWithError(nerr)
	}()

	// Setup cleanup function to cause the above go-routine to
	// exit in case of partial read
	pipeCloser := func() { pr.Close() }
	return minio.NewGetObjectReaderFromReader(pr, objInfo, opts, pipeCloser)
}

func (w *weedObjects) ListBuckets(ctx context.Context) (buckets []minio.BucketInfo, err error) {

	entries, _, err := w.list(BucketDir, "", "", false, math.MaxInt32)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDirectory {
			buckets = append(buckets, minio.BucketInfo{
				Name:    entry.Name,
				Created: time.Unix(entry.Attributes.Crtime, 0),
			})
		}
	}
	return buckets, nil
}

func (w *weedObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int,
	fetchOwner bool, startAfter string) (loi minio.ListObjectsV2Info, err error) {
	// fetchOwner is not supported and unused.
	marker := continuationToken
	if marker == "" {
		marker = startAfter
	}
	resultV1, err := w.ListObjects(ctx, bucket, prefix, marker, delimiter, maxKeys)
	if err != nil {
		return loi, err
	}
	return minio.ListObjectsV2Info{
		Objects:               resultV1.Objects,
		Prefixes:              resultV1.Prefixes,
		ContinuationToken:     continuationToken,
		NextContinuationToken: resultV1.NextMarker,
		IsTruncated:           resultV1.IsTruncated,
	}, nil
}

func (w *weedObjects) ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (loi minio.ListObjectsInfo, err error) {
	var mutex sync.Mutex

	getObjectInfo := func(ctx context.Context, bucket, entry string) (objectInfo minio.ObjectInfo, err error) {
		mutex.Lock()
		defer mutex.Unlock()

		path := util.NewFullPath(w.weedPathJoin(bucket), entry)

		ei, err := filer_pb.GetEntry(w.Client, path)
		if err == filer_pb.ErrNotFound {
			return objectInfo, minio.ObjectNotFound{Bucket: bucket, Object: entry}
		} else if err != nil {
			return objectInfo, err
		}
		ei.Name = entry
		objectInfo = entryInfoToObjectInfo(bucket, ei)

		return objectInfo, nil
	}

	return minio.ListObjects(ctx, w, bucket, prefix, marker, delimiter, maxKeys, w.listPool, w.listDirFactory(), w.isLeaf, w.isLeafDir, getObjectInfo, getObjectInfo)
}

func (w *weedObjects) listDirFactory() minio.ListDirFunc {
	listDir := func(bucket, prefixDir, prefixEntry string) (emptyDir bool, entries []string, delayIsLeaf bool) {
		path := w.weedPathJoin(bucket, prefixDir)

		fis, _, err := w.list(path, "", "", false, math.MaxInt32)
		if err != nil {
			return false, entries, false
		}
		if len(fis) == 0 {
			return true, nil, false
		}
		for _, fi := range fis {
			if fi.IsDirectory {
				entries = append(entries, fi.Name+weedSeparator)
			} else {
				entries = append(entries, fi.Name)
			}
		}

		entries, delayIsLeaf = minio.FilterListEntries(bucket, prefixDir, entries, prefixEntry, w.isLeaf)
		return false, entries, delayIsLeaf
	}

	return listDir
}

func (w *weedObjects) list(path, prefix, startFrom string, inclusive bool, limit uint32) (entries []*filer_pb.Entry, isLast bool, err error) {

	err = filer_pb.List(w.Client, path, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
		entries = append(entries, entry)
		if isLastEntry {
			isLast = true
		}
		return nil
	}, startFrom, inclusive, limit)

	if len(entries) == 0 {
		isLast = true
	}
	return
}

func entryInfoToObjectInfo(bucket string, entry *filer_pb.Entry) minio.ObjectInfo {
	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    entry.Name,
		ModTime: time.Unix(entry.Attributes.Mtime, 0),
		Size:    int64(entry.Attributes.FileSize),
		IsDir:   entry.GetIsDirectory(),
		AccTime: time.Time{},
	}
}

func (w *weedObjects) isLeafDir(bucket, leafPath string) bool {
	return w.isObjectDir(context.Background(), bucket, leafPath)
}

func (w *weedObjects) isLeaf(bucket, leafPath string) bool {
	return !strings.HasSuffix(leafPath, weedSeparator)
}

func (w *weedObjects) isObjectDir(ctx context.Context, bucket, prefix string) bool {
	isdir, err := filer_pb.Exists(w.Client, w.weedPathJoin(bucket), prefix, true)
	if !isdir || err != nil {
		return false
	}
	entries, _, err := w.list(w.weedPathJoin(bucket, prefix), "", "", false, math.MaxInt32)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

func (w *weedObjects) isEmptyPath(ctx context.Context, path string) bool {

	entries, _, err := w.list(path, "", "", false, math.MaxInt32)
	if err != nil {
		return false
	}
	return len(entries) == 0
}

func (w *weedObjects) DeleteBucket(ctx context.Context, bucket string, opts minio.DeleteBucketOptions) error {
	if err := filer_pb.Remove(w.Client, BucketDir, bucket, true, true, true, false, nil); err != nil {
		return err
	}
	return nil
}

func (w *weedObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	if strings.HasSuffix(object, weedSeparator) && !w.isObjectDir(ctx, bucket, object) {
		return objInfo, minio.ObjectNotFound{Bucket: bucket, Object: object}
	}
	if err := w.deleteObject(w.weedPathJoin(bucket), w.weedPathJoin(bucket, object)); err != nil {
		return objInfo, err
	}
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
}

func (w *weedObjects) deleteObject(basePath, deletePath string) error {
	if basePath == deletePath {
		return nil
	}

	if err := filer_pb.Remove(w.Client, "", deletePath, true, true, true, false, nil); err != nil {
		return err
	}

	deletePath = strings.TrimSuffix(deletePath, weedSeparator)
	deletePath = path.Dir(deletePath)

	if !w.isEmptyPath(context.Background(), deletePath+weedSeparator) {
		return nil
	}

	w.deleteObject(basePath, deletePath)

	return nil
}

func (w *weedObjects) DeleteObjects(ctx context.Context, bucket string, objects []minio.ObjectToDelete, opts minio.ObjectOptions) ([]minio.DeletedObject, []error) {
	errs := make([]error, len(objects))
	dobjects := make([]minio.DeletedObject, len(objects))
	for idx, object := range objects {
		_, errs[idx] = w.DeleteObject(ctx, bucket, object.ObjectName, opts)
		if errs[idx] == nil {
			dobjects[idx] = minio.DeletedObject{
				ObjectName: object.ObjectName,
			}
		}
	}
	return dobjects, errs
}

func (w *weedObjects) GetBucketInfo(ctx context.Context, bucket string) (bi minio.BucketInfo, err error) {
	path := util.NewFullPath(BucketDir, bucket)
	entry, err := filer_pb.GetEntry(w.Client, path)
	if err != nil {
		return bi, err
	}

	return minio.BucketInfo{
		Name:    bucket,
		Created: time.Unix(entry.GetAttributes().GetCrtime(), 0),
	}, nil
}

func (w *weedObjects) MakeBucketWithLocation(ctx context.Context, bucket string, opts minio.BucketOptions) error {
	if s3utils.CheckValidBucketName(bucket) != nil {
		return minio.BucketNameInvalid{Bucket: bucket}
	}
	if opts.LockEnabled || opts.VersioningEnabled {
		return minio.NotImplemented{}
	}
	if exist, err := filer_pb.Exists(w.Client, BucketDir, bucket, true); err == nil && exist {
		return minio.BucketExists{}
	}
	if err := filer_pb.Mkdir(w.Client, BucketDir, bucket, nil); err != nil {
		return minio.ErrorRespToObjectError(err, bucket)
	}
	return nil
}

func (w *weedObjects) PutObject(ctx context.Context, bucket string, object string, r *minio.PutObjReader, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	objectName := strings.TrimSuffix(object, weedSeparator)
	path := w.weedPathJoin(bucket, objectName)
	if strings.HasSuffix(object, weedSeparator) && r.Size() == 0 {
		if err := filer_pb.Mkdir(w.Client, "", path, nil); err != nil {
			return minio.ObjectInfo{}, err
		}
	} else {
		uploadURL := fmt.Sprintf("http://%s%s", w.Client.option.Filer, path)
		client := &http.Client{}
		data := r.Reader

		req, err := http.NewRequest("PUT", uploadURL, data)

		if err != nil {
			return minio.ObjectInfo{}, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return minio.ObjectInfo{}, err
		}
		defer resp.Body.Close()
	}
	fi, err := w.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	return fi, nil
}

func (w *weedObjects) Shutdown(ctx context.Context) error {
	return nil
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

func (w *weedObjects) CopyObject(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.ObjectInfo, error) {
	cpSrcDstSame := minio.IsStringEqual(w.weedPathJoin(srcBucket, srcObject), w.weedPathJoin(dstBucket, dstObject))
	if cpSrcDstSame {
		return w.GetObjectInfo(ctx, srcBucket, srcObject, minio.ObjectOptions{})
	}

	return w.PutObject(ctx, dstBucket, dstObject, srcInfo.PutObjReader, minio.ObjectOptions{
		ServerSideEncryption: dstOpts.ServerSideEncryption,
		UserDefined:          srcInfo.UserDefined,
	})
}

func (w *weedObjects) NewMultipartUpload(ctx context.Context, bucket string, object string, opts minio.ObjectOptions) (uploadID string, err error) {
	exists, err := filer_pb.Exists(w.Client, BucketDir, bucket, true)
	if err != nil {
		return uploadID, err
	}
	if !exists {
		return uploadID, minio.BucketNotFound{}
	}

	minioMetaTmpBucketExists, err := filer_pb.Exists(w.Client, w.weedPathJoin(minioMetaBucket), minioMetaTmpDir, true)
	if err != nil {
		return uploadID, err
	}
	if !minioMetaTmpBucketExists {
		if err = filer_pb.Mkdir(w.Client, w.weedPathJoin(minioMetaBucket), minioMetaTmpDir, nil); err != nil {
			return uploadID, err
		}
	}

	//var entry = filer_pb.Entry{}

	uploadID = minio.MustGetUUID()
	//entry.Name = uploadID
	////err = filer_pb.MkFile(w.Client, multipartUploadDir, uploadID, nil, nil)

	//if err = filer_pb.Touch(w.Client, multipartUploadDir, uploadID, &entry); err != nil {
	//	return uploadID, err
	//}
	//tmpObject := strings.NewReader("")
	//_, err = w.PutObject(ctx, minioMetaBucket, fmt.Sprintf("%s/%s", minioMetaTmpDir, uploadID), tmpObject, opts)
	//if err != nil {
	//	return "", err
	//}
	path := fmt.Sprintf("%s/%s", w.weedPathJoin(minioMetaTmpBucket), uploadID)
	uploadURL := fmt.Sprintf("http://%s%s", w.Client.option.Filer, path)
	client := &http.Client{}
	req, err := http.NewRequest("PUT", uploadURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	return uploadID, nil
}

func (w *weedObjects) ListMultipartUploads(ctx context.Context, bucket string, prefix string, keyMarker string, uploadIDMarker string, delimiter string, maxUploads int) (lmi minio.ListMultipartsInfo, err error) {
	exists, err := filer_pb.Exists(w.Client, BucketDir, bucket, true)
	if err != nil {
		return lmi, err
	}
	if !exists {
		return lmi, minio.BucketNotFound{}
	}

	// not implemented, return empty
	return lmi, nil
}

func (w *weedObjects) checkUploadIDExists(ctx context.Context, bucket, object, uploadID string) (err error) {
	exists, err := filer_pb.Exists(w.Client, BucketDir, bucket, true)
	if err != nil {
		return err
	}
	if !exists {
		return minio.BucketNotFound{}
	}

	exists, err = filer_pb.Exists(w.Client, multipartUploadDir, uploadID, false)
	if err != nil {
		return err
	}
	if !exists {
		return minio.ObjectNotFound{}
	}
	return nil
}

// GetMultipartInfo returns multipart info of the uploadId of the object
func (w *weedObjects) GetMultipartInfo(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (result minio.MultipartInfo, err error) {
	exists, err := filer_pb.Exists(w.Client, BucketDir, bucket, true)
	if err != nil {
		return result, err
	}
	if !exists {
		return result, minio.BucketNotFound{}
	}

	if err = w.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	result.Bucket = bucket
	result.Object = object
	result.UploadID = uploadID
	return result, nil
}

func (w *weedObjects) ListObjectParts(ctx context.Context, bucket, object, uploadID string, partNumberMarker int, maxParts int, opts minio.ObjectOptions) (result minio.ListPartsInfo, err error) {
	exists, err := filer_pb.Exists(w.Client, BucketDir, bucket, true)
	if err != nil {
		return result, err
	}
	if !exists {
		return result, minio.BucketNotFound{}
	}

	if err = w.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return result, err
	}

	// not implemented, return empty
	return result, nil
}

func (w *weedObjects) PutObjectPart(ctx context.Context, bucket, object, uploadID string, partID int, r *minio.PutObjReader, opts minio.ObjectOptions) (info minio.PartInfo, err error) {
	exists, err := filer_pb.Exists(w.Client, BucketDir, bucket, true)
	if err != nil {
		return info, err
	}
	if !exists {
		return info, minio.BucketNotFound{}
	}

	path := fmt.Sprintf("%s/%s", multipartUploadDir, uploadID)
	uploadURL := fmt.Sprintf("http://%s/%s", w.Client.option.Filer, path)
	client := &http.Client{}
	data := r.Reader

	req, err := http.NewRequest("PUT", uploadURL, data)
	if err != nil {
		return minio.PartInfo{}, err
	}
	req.Form.Add("op", "append")

	resp, err := client.Do(req)
	if err != nil {
		return minio.PartInfo{}, err
	}

	defer resp.Body.Close()

	info.PartNumber = partID
	info.ETag = r.MD5CurrentHexString()
	info.LastModified = minio.UTCNow()
	info.Size = r.Reader.Size()

	return info, nil
}

func (w *weedObjects) CopyObjectPart(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject, uploadID string, partID int,
	startOffset int64, length int64, srcInfo minio.ObjectInfo, srcOpts, dstOpts minio.ObjectOptions) (minio.PartInfo, error) {
	return w.PutObjectPart(ctx, dstBucket, dstObject, uploadID, partID, srcInfo.PutObjReader, dstOpts)
}

func (w *weedObjects) CompleteMultipartUpload(ctx context.Context, bucket, object, uploadID string, parts []minio.CompletePart, opts minio.ObjectOptions) (objInfo minio.ObjectInfo, err error) {
	exists, err := filer_pb.Exists(w.Client, BucketDir, bucket, true)
	if err != nil {
		return objInfo, err
	}
	if !exists {
		return objInfo, minio.BucketNotFound{}
	}

	if err = w.checkUploadIDExists(ctx, bucket, object, uploadID); err != nil {
		return objInfo, err
	}

	name := w.weedPathJoin(bucket, object)
	dir := path.Dir(name)

	if dir != "" {
		if err = filer_pb.Mkdir(w.Client, "", dir, nil); err != nil {
			return objInfo, err
		}
	}

	//	# move(rename) "/path/to/src_file" to "/path/to/dst_file"
	//	> curl -X POST 'http://localhost:8888/path/to/dst_file?mv.from=/path/to/src_file'
	renameURL := fmt.Sprintf("http://%s/%s", w.Client.option.Filer, w.weedPathJoin(bucket, object))
	data := url.Values{"mv.from": {fmt.Sprintf("%s/%s", multipartUploadDir, uploadID)}}

	resp, err := http.PostForm(renameURL, data)
	if err != nil {
		return objInfo, err
	}

	defer resp.Body.Close()

	s3MD5 := minio.ComputeCompleteMultipartMD5(parts)
	path := util.NewFullPath(w.weedPathJoin(bucket), object)

	entry, err := filer_pb.GetEntry(w.Client, path)
	if err == filer_pb.ErrNotFound {
		return objInfo, minio.ObjectNotFound{Bucket: bucket, Object: object}
	} else if err != nil {
		return objInfo, err
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    s3MD5,
		ModTime: time.Unix(entry.Attributes.Mtime, 0),
		Size:    int64(entry.Attributes.FileSize),
		IsDir:   entry.IsDirectory,
		AccTime: time.Time{},
	}, nil
}

func (w *weedObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string, opts minio.ObjectOptions) (err error) {
	path := w.weedPathJoin(bucket, object)
	if err := filer_pb.Remove(w.Client, "", path, true, true, true, false, nil); err != nil {
		return err
	}
	return fmt.Errorf("Multipart upload aborted")

}
