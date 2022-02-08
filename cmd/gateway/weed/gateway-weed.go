package weed

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
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
		Metrics: metrics,
	}
	return &weedGateway, nil
}

type weedObjects struct {
	minio.GatewayUnsupported
	Client     *WeedClient
	HTTPClient *http.Client
	Metrics    *minio.BackendMetrics
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
	entry, err := filer_pb.GetEntry(w.Client, path)
	if err != nil {
		return objInfo, err
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

	var objects []minio.ObjectInfo
	path := w.weedPathJoin(bucket, prefix)

	entries, _, err := w.list(path, "", "", false, math.MaxInt32)
	if err != nil {
		return loi, err
	}

	for _, entry := range entries {
		if prefix != "" {
			entry.Name = prefix + entry.Name
		}
		objects = append(objects, entryInfoToObjectInfo(bucket, entry))
	}

	return minio.ListObjectsInfo{
		IsTruncated: false,
		NextMarker:  "",
		Objects:     objects,
		Prefixes:    []string{},
	}, nil
}

func (w *weedObjects) list(parentDirectoryPath, prefix, startFrom string, inclusive bool, limit uint32) (entries []*filer_pb.Entry, isLast bool, err error) {

	err = filer_pb.List(w.Client, parentDirectoryPath, prefix, func(entry *filer_pb.Entry, isLastEntry bool) error {
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

func (w *weedObjects) DeleteBucket(ctx context.Context, bucket string, opts minio.DeleteBucketOptions) error {
	if err := filer_pb.Remove(w.Client, BucketDir, bucket, true, true, true, false, nil); err != nil {
		return err
	}
	return nil
}

func (w *weedObjects) DeleteObject(ctx context.Context, bucket, object string, opts minio.ObjectOptions) (minio.ObjectInfo, error) {
	path := w.weedPathJoin(bucket)
	if err := filer_pb.Remove(w.Client, path, object, true, true, true, false, nil); err != nil {
		return minio.ObjectInfo{}, err
	}
	return minio.ObjectInfo{
		Bucket: bucket,
		Name:   object,
	}, nil
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
	path := w.weedPathJoin(bucket, object)
	if strings.HasSuffix(object, weedSeparator) && r.Size() == 0 {
		if err := filer_pb.Mkdir(w.Client, BucketDir, path, nil); err != nil {
			return minio.ObjectInfo{}, err
		}
	}
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
	fi, err := w.GetObjectInfo(ctx, bucket, object, opts)
	if err != nil {
		return minio.ObjectInfo{}, err
	}

	return minio.ObjectInfo{
		Bucket:  bucket,
		Name:    object,
		ETag:    r.MD5CurrentHexString(),
		ModTime: fi.ModTime,
		Size:    fi.Size,
		IsDir:   fi.IsDir,
		AccTime: fi.AccTime,
	}, nil
}

func (w *weedObjects) Shutdown(ctx context.Context) error {
	return nil
}

func (w *weedObjects) StorageInfo(ctx context.Context) (si minio.StorageInfo, errs []error) {
	return minio.StorageInfo{
		Disks:   []madmin.Disk{},
		Backend: madmin.BackendInfo{},
	}, nil
}
