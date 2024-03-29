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
	Addr   string `mapstructure:"addr"`
	Weight int    `mapstructure:"weight"`
}

type Server struct {
	Name        string           `mapstructure:"name"`
	Endpoints   []EndpointServer `mapstructure:"endpoint"`
	Priority    int              `mapstructure:"priority"`
	Timeout     int              `mapstructure:"timeout"`
	LoadBalance string           `mapstructure:"lb"`
	Pool        *PoolConfig      `mapstructure:"pool"`
}
type PoolConfig struct {
	MaxOpenConns int `mapstructure:"max_open_conns"`
	MaxIdleConns int `mapstructure:"max_idle_conns"`
	Size         int `mapstructure:"size"`
	Timeout      int `mapstructure:"timeout"`

	MinIdleConns   int `mapstructure:"min_idle_conns"`
	MaxActiveConns int `mapstructure:"max_active_conns"`
}

// NewGrpcConnPool 创建一个grpc连接池
func NewGrpcConnPool(addr string, poolConfig *PoolConfig, poolOpt ...PoolOptionsBuildOption) Pool {
	opts := NewPoolOptions(func(ctx context.Context) (Connection, error) {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		return NewConnectionImpl(conn, 30*time.Second, 10*time.Second), nil
	})

	if poolConfig.MaxIdleConns > 0 {
		opts.MaxIdleConns = int32(poolConfig.MaxIdleConns)
	}
	if poolConfig.Timeout > 0 {
		opts.PoolTimeout = time.Duration(poolConfig.Timeout) * time.Second
	}
	if poolConfig.Size > 0 {
		opts.PoolSize = int32(poolConfig.Size)
	}
	if poolConfig.MaxActiveConns > 0 {
		opts.MaxActiveConns = int32(poolConfig.MaxActiveConns)
	}
	if poolConfig.MinIdleConns > 0 {
		opts.MinIdleConns = int32(poolConfig.MinIdleConns)
	}

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

func NewGrpcLoadBalance(serverConfig *Server) LoadBalance {
	var endpoints []Endpoint
	for _, endpoint := range serverConfig.Endpoints {
		pool := NewGrpcConnPool(endpoint.Addr, serverConfig.Pool)
		endpoints = append(endpoints, NewGrpcEndpoint(endpoint.Addr, pool))
	}
	switch BalanceType(serverConfig.LoadBalance) {
	case WRRBalanceType:
		return NewWeightedRoundRobinBalance(endpoints)
	case RRBalanceType:
		return NewRoundRobinBalance(endpoints)
	case NQBalanceType:
		return NewNeverQueueBalance(endpoints)
	case WLCBalanceType:
		return NewLeastConnectionsBalance(endpoints)
	case SEDBalanceType:
		return NewShortestExpectedDelayBalance(endpoints)
	case ConsistentHashBalanceType:
		return NewConsistentHashBalancer(endpoints, 4)
	default:
		return NewRoundRobinBalance(endpoints)
	}
}
