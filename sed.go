package goloadbalancer

import (
	"sync"
)

type ShortestExpectedDelayEndpoint interface {
	Endpoint
	ActivateConnections() int
	Weight() int
}

type ShortestExpectedDelayBalance struct {
	*BaseLoadBalance
}

func NewShortestExpectedDelayBalance(endpoints []Endpoint) LoadBalance {
	return &ShortestExpectedDelayBalance{
		&BaseLoadBalance{
			endpoints: endpoints,
			lock:      sync.RWMutex{},
		},
	}
}

func (l *ShortestExpectedDelayBalance) Select(args ...interface{}) (Endpoint, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if len(l.endpoints) == 0 {
		return nil, ErrNoEndpoint
	}
	min := l.endpoints[0].(ShortestExpectedDelayEndpoint)
	// Overhead = （ACTIVE+1）*256/Weight
	minOverhead := float64((min.ActivateConnections()+1)*256) / float64(min.Weight())
	for _, endpoint := range l.endpoints {
		endpoint := endpoint.(ShortestExpectedDelayEndpoint)
		overhead := float64((endpoint.ActivateConnections()+1)*256) / float64(endpoint.Weight())
		if overhead < minOverhead {
			min = endpoint
			minOverhead = overhead
		}
	}
	return min, nil
}

func (l *ShortestExpectedDelayBalance) Name() string {
	return string(SEDBalanceType)
}
