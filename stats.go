package goloadbalancer

import "sync/atomic"

type Stats interface{
	GetActivateConns() int
	GetIdleConns() int
}

// Stats contains pool state information and accumulated stats.
type StatsImpl struct {
	Hits     uint32 // number of times free connection was found in the pool
	Misses   uint32 // number of times free connection was NOT found in the pool
	Timeouts uint32 // number of times a wait timeout occurred

	TotalConns   uint32 // number of total connections in the pool
	IdleConns    uint32 // number of idle connections in the pool
	StaleConns   uint32 // number of stale connections removed from the pool
	InvalidConns uint32 // number of invalid connections removed from the pool
	InUsedConns  int32 // number of connections in used
}

func (s *StatsImpl) GetActivateConns() int {
	return int(atomic.LoadInt32(&s.InUsedConns))
}

func (s *StatsImpl) GetIdleConns() int {
	return int(atomic.LoadUint32(&s.IdleConns))
}