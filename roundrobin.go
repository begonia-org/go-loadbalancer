package goloadbalancer

import (
	"reflect"
	"sync"
)

// RoundRobinBalance 轮询负载均衡
// 轮询(Round Robin): 轮询算法是一种分配算法，
// 它将每个新的请求按顺序分配给下一个服务器。当到达列表末尾时，算法再从列表的开始处继续分配。
type RoundRobinBalance struct {
	curIndex  int
	endpoints []Endpoint
	lock      sync.Mutex // 用于确保并发安全
}

func NewRoundRobinBalance(endpoints []Endpoint) LoadBalance {
	return &RoundRobinBalance{
		endpoints: endpoints,
		curIndex:  0,
		lock:      sync.Mutex{},
	}
}
func (r *RoundRobinBalance) GetEndpoints() []Endpoint {
	return r.endpoints
}
func (r *RoundRobinBalance) AddEndpoint(endpoint interface{}) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.endpoints = append(r.endpoints, endpoint.(Endpoint))
}
func (r *RoundRobinBalance) Select(args ...interface{}) (Endpoint, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if len(r.endpoints) == 0 {
		return nil, ErrNoEndpoint
	}
	endpoint := r.endpoints[r.curIndex]
	r.curIndex = (r.curIndex + 1) % len(r.endpoints)
	return endpoint, nil
}

func (r *RoundRobinBalance) RemoveEndpoint(endpoint Endpoint) {
	r.lock.Lock()
	defer r.lock.Unlock()
	for i, ep := range r.endpoints {
		if reflect.DeepEqual(ep, endpoint) {
			r.endpoints = append(r.endpoints[:i], r.endpoints[i+1:]...)
			return
		}
	}
}

func (r *RoundRobinBalance) Name() string {
	return "RoundRobin"
}

func (r *RoundRobinBalance) Close() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	for _, endpoint := range r.endpoints {
		if err := endpoint.Close(); err != nil {
			return err
		}
	}
	return nil
}
