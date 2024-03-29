package goloadbalancer

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// HashCircle 一致性哈希环
type HashCircle map[uint32]Endpoint

type ConsistentHashBalancer struct {
	circle     HashCircle
	sortedKeys []uint32
	vNodes     int                      // 每个实际节点对应的虚拟节点数量
	hashFn     func(data []byte) uint32 // 哈希函数
	*BaseLoadBalance
}

func NewConsistentHashBalancer(endpoints []Endpoint, vNodes int) LoadBalance {
	b := &ConsistentHashBalancer{
		circle:     make(HashCircle),
		sortedKeys: []uint32{},

		vNodes: vNodes,
		hashFn: crc32.ChecksumIEEE,
		BaseLoadBalance: &BaseLoadBalance{
			endpoints: endpoints,
			lock:      sync.RWMutex{},
		},
	}
	for _, endpoint := range endpoints {
		b.AddEndpoint(endpoint)
	}
	return b
}

func (c *ConsistentHashBalancer) AddEndpoint(ep interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()
	endpoint := ep.(Endpoint)
	// 每个实际节点对应vNodes个虚拟节点
	for i := 0; i < c.vNodes; i++ {
		vNodeKey := fmt.Sprintf("%s#%d", endpoint.Addr(), i)
		hash := c.hashFn([]byte(vNodeKey))
		c.circle[hash] = endpoint
		c.sortedKeys = append(c.sortedKeys, hash)
	}
	sort.Slice(c.sortedKeys, func(i, j int) bool { return c.sortedKeys[i] < c.sortedKeys[j] })
}
func (c *ConsistentHashBalancer) Select(args ...interface{}) (Endpoint, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	if len(c.endpoints) == 0 {
		return nil, ErrNoEndpoint
	}
	if len(args) == 0 {
		return nil, ErrNoSourceIP
	}
	sourceIP := args[0].(string)
	hash := c.hashFn([]byte(sourceIP))
	idx := sort.Search(len(c.sortedKeys), func(i int) bool { return c.sortedKeys[i] >= hash })
	if idx == len(c.sortedKeys) {
		idx = 0
	}
	return c.circle[c.sortedKeys[idx]], nil
}

func (c *ConsistentHashBalancer) RemoveEndpoint(endpoint Endpoint) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i := 0; i < c.vNodes; i++ {
		vNodeKey := fmt.Sprintf("%s#%d", endpoint.Addr(), i)
		hash := c.hashFn([]byte(vNodeKey))
		if _, exists := c.circle[hash]; exists {
			delete(c.circle, hash)
			index := sort.Search(len(c.sortedKeys), func(i int) bool { return c.sortedKeys[i] == hash })
			c.sortedKeys = append(c.sortedKeys[:index], c.sortedKeys[index+1:]...)
		}
	}
}
func (c *ConsistentHashBalancer) Name() string {
	return string(ConsistentHashBalanceType)
}
