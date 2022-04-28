package weed

import (
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
)

type WeedOptions struct {
	Filer          pb.ServerAddress
	MasterClient   *wdclient.MasterClient
	GrpcDialOption grpc.DialOption
}

type WeedClient struct {
	option *WeedOptions
}

func NewWeedClient(option *WeedOptions) (weedClient *WeedClient, err error) {
	weedClient = &WeedClient{
		option: option,
	}
	return weedClient, nil
}

func (w *WeedClient) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithGrpcClient(false, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, w.option.Filer.ToGrpcAddress(), w.option.GrpcDialOption)

}

func (w *WeedClient) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}
