package goloadbalancer

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type grpcEndpointImpl struct {
	addr string
	pool Pool
}
type EndpointServer struct {
	Addr   string
	Weight int
}

// NewGrpcConnPool 创建一个grpc连接池
func NewGrpcConnPool(addr string, poolOpt ...PoolOptionsBuildOption) Pool {
	opts := NewPoolOptions(func(ctx context.Context) (Connection, error) {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return NewConnectionImpl(conn, 30*time.Second, 10*time.Second), nil
	})
	opts.ConnectionUsedHook = append(opts.ConnectionUsedHook, ConnUseAt)
	for _, opt := range poolOpt {
		opt(opts)

	}
	pool := NewConnPool(opts)
	return pool

}

func NewGrpcEndpoint(addr string, pool Pool) Endpoint {
	return &grpcEndpointImpl{
		addr: addr,
		pool: pool,
	}
}
func (g *grpcEndpointImpl) AfterTransform(ctx context.Context, cn Connection) {
	g.pool.Release(ctx, cn)
}
func (g *grpcEndpointImpl) Stats() Stats {
	return g.pool.Stats()
}
func (g *grpcEndpointImpl) Addr() string {
	return g.addr
}

func (g *grpcEndpointImpl) Get(ctx context.Context) (interface{}, error) {
	return g.pool.Get(ctx)
}
func (g *grpcEndpointImpl) Close() error {
	return g.pool.Close()
}
