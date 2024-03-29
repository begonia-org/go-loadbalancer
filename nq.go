package goloadbalancer

type NeverQueueBalance struct {
	*ShortestExpectedDelayBalance
}

func NewNeverQueueBalance(endpoints []Endpoint) LoadBalance {
	return &NeverQueueBalance{
		NewShortestExpectedDelayBalance(endpoints).(*ShortestExpectedDelayBalance),
	}
}
func (n *NeverQueueBalance) Select(args ...interface{}) (Endpoint, error) {
	n.lock.RLock()
	defer n.lock.RUnlock()
	if len(n.endpoints) == 0 {
		return nil, ErrNoEndpoint
	}
	endpoint := n.zeroConn()
	if endpoint != nil {
		return endpoint, nil

	}
	min := n.endpoints[0].(ShortestExpectedDelayEndpoint)
	// Overhead = （ACTIVE+1）*256/Weight
	minOverhead := float64((min.ActivateConnections()+1)*256) / float64(min.Weight())
	for _, endpoint := range n.endpoints {
		endpoint := endpoint.(ShortestExpectedDelayEndpoint)
		overhead := float64((endpoint.ActivateConnections()+1)*256) / float64(endpoint.Weight())
		if overhead < minOverhead {
			min = endpoint
			minOverhead = overhead
		}
	}
	return min, nil
}
func (n *NeverQueueBalance) zeroConn() ShortestExpectedDelayEndpoint {
	for _, endpoint := range n.endpoints {
		endpoint := endpoint.(ShortestExpectedDelayEndpoint)
		if endpoint.ActivateConnections() == 0 {
			return endpoint
		}
	}
	return nil
}

func (n *NeverQueueBalance) Name() string {
	return string(NQBalanceType)
}
