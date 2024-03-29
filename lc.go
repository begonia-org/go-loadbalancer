package goloadbalancer

import (
	"sync"
)

type LeastConnectionsEndpoint interface {
	Endpoint
	ActivateConnections() int
	InactiveConnections() int
}

// LeastConnectionsBalance 最小连接数负载均衡
type LeastConnectionsBalance struct {
	*BaseLoadBalance
}

// NewLeastConnectionsBalance 创建一个最小连接数负载均衡器
func NewLeastConnectionsBalance(endpoints []Endpoint) LoadBalance {
	return &LeastConnectionsBalance{
		&BaseLoadBalance{
			endpoints: endpoints,
			lock:      sync.RWMutex{},
		},
	}
}

func (l *LeastConnectionsBalance) Select(args ...interface{}) (Endpoint, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if len(l.endpoints) == 0 {
		return nil, ErrNoEndpoint
	}
	min := l.endpoints[0].(LeastConnectionsEndpoint)
	// Overhead =（ Active * 256 + Inactive ）/Weight
	minOverhead := min.ActivateConnections()*256 + min.InactiveConnections()
	for _, endpoint := range l.endpoints {
		endpoint := endpoint.(LeastConnectionsEndpoint)
		overhead := endpoint.ActivateConnections()*256 + endpoint.InactiveConnections()
		if overhead < minOverhead {
			min = endpoint
			minOverhead = overhead
		}
	}
	return min, nil
}

func (l *LeastConnectionsBalance) Name() string {
	return string(LCBalanceType)
}
