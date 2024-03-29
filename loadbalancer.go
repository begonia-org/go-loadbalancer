package goloadbalancer

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

)

type BalanceType string

const (
	RRBalanceType             BalanceType = "RoundRobin"
	WRRBalanceType            BalanceType = "WeightedRoundRobin"
	ConsistentHashBalanceType BalanceType = "ConsistentHash"
	LCBalanceType             BalanceType = "LeastConnection"
	SEDBalanceType            BalanceType = "ShortestExpectedDelay"
	WLCBalanceType            BalanceType = "WeightedLeastConnection"
	NQBalanceType             BalanceType = "NeverQueue"
)

type EndpointMeta interface {
	Addr() string
	Weight() int
}
type Endpoint interface {
	Get(context.Context) (interface{}, error)
	Addr() string
	Close() error
	Stats() Stats
	AfterTransform(ctx context.Context, cn Connection)
}
type LoadBalance interface {
	Select(args ...interface{}) (Endpoint, error)
	AddEndpoint(endpoint interface{})
	RemoveEndpoint(endpoint Endpoint)
	Name() string
	Close() error
	GetEndpoints() []Endpoint
}
type BaseLoadBalance struct {
	endpoints []Endpoint
	lock      sync.RWMutex
	closed    uint32
}

func (b *BaseLoadBalance) GetEndpoints() []Endpoint {
	return b.endpoints
}
func (b *BaseLoadBalance) AddEndpoint(endpoint interface{}) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.endpoints = append(b.endpoints, endpoint.(Endpoint))

}
func (b *BaseLoadBalance) RemoveEndpoint(endpoint Endpoint) {
	b.lock.Lock()
	defer b.lock.Unlock()
	for i, ep := range b.endpoints {
		if ep == endpoint {
			b.endpoints = append(b.endpoints[:i], b.endpoints[i+1:]...)
			return
		}
	}

}
func (b *BaseLoadBalance) Close() error {
	if !atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		// 如果已经是关闭状态，则直接返回，表示无需再次关闭
		return nil
	}
	for _, endpoint := range b.endpoints {
		if err := endpoint.Close(); err != nil {
			return err
		}
	}
	return nil
}

var (
	ErrNoEndpoint = errors.New("no endpoint available")
	ErrNoSourceIP = errors.New("no source ip")
)
