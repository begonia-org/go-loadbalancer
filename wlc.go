package goloadbalancer

import (
	"sync"
)

type WeightLeastConnectionsEndpoint interface {
	Endpoint
	LeastConnectionsEndpoint
	Weight() int
}
type WeightLeastConnectionsBalance struct {
	// lock      sync.Mutex
	// endpoints []Endpoint
	*BaseLoadBalance
}

func NewWeightLeastConnectionsBalance(endpoints []Endpoint) *WeightLeastConnectionsBalance {
	return &WeightLeastConnectionsBalance{
		&BaseLoadBalance{
			endpoints: endpoints,
			lock:      sync.RWMutex{},
		},
	}
}

func (l *WeightLeastConnectionsBalance) Select(_ ...interface{}) (Endpoint, error) {
	l.lock.RLock()
	defer l.lock.RUnlock()
	if len(l.endpoints) == 0 {
		return nil, ErrNoEndpoint
	}
	min := l.endpoints[0].(WeightLeastConnectionsEndpoint)
	// Overhead =（ Active * 256 + Inactive ）/Weight
	minRatio := float64(min.ActivateConnections()*256+min.InactiveConnections()) / float64(min.Weight())
	for _, ep := range l.endpoints {
		endpoint := ep.(WeightLeastConnectionsEndpoint)
		ratio := float64(endpoint.Weight()) / float64(endpoint.ActivateConnections()+1)

		if ratio < minRatio {
			min = endpoint
			minRatio = ratio
		}
	}
	return min, nil
}

func (r *WeightLeastConnectionsBalance) Name() string {
	return string(WLCBalanceType)
}
