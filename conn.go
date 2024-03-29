package goloadbalancer

import (
	"sync"
	"time"
)

type Conn interface {
	// Close closes the connection.
	Close() error
}
type ConnectionImpl struct {
	conn      Conn
	usedAt    time.Time // atomic
	createdAt time.Time
	// 连接的最大存活时间，超过这个时间的连接将会关闭
	// 在高并发的环境中，使用连接过期策略可以帮助均衡每个连接的使用频率，防止某些连接过度使用而其他连接闲置。这有助于提高连接池的整体效率和性能。
	// 这个机制确保了连接池的健康和稳定性，同时减少了因长期使用同一连接可能出现的问题。
	maxLifetime time.Duration
	// 最大空闲时间，超过这个时间的连接将会关闭
	connMaxIdleTime time.Duration
	isClosed        bool
	mu              sync.Mutex
	inUsed		   bool

	// 连接的最大空闲时间，超过这个时间的连接将会关闭
}

func NewConnectionImpl(conn Conn, maxLifetime, connMaxIdleTime time.Duration) *ConnectionImpl {
	return &ConnectionImpl{
		conn:            conn,
		createdAt:       time.Now(),
		usedAt:          time.Now(),
		maxLifetime:     maxLifetime,
		connMaxIdleTime: connMaxIdleTime,
		inUsed: 		false,
	}
}

func (c *ConnectionImpl) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed {
		return nil
	}
	c.isClosed = true
	return c.conn.Close()
}
func (c *ConnectionImpl) IsUsing() bool {
	return c.inUsed
}
func (c *ConnectionImpl) InUsed(inUsed bool) {
	c.inUsed = inUsed
}
func (c *ConnectionImpl) ConnInstance() Conn {

	return c.conn
}
func (c *ConnectionImpl) Validate() bool {
	if c.isClosed {
		return false
	}
	if c.maxLifetime > 0 && time.Since(c.createdAt) > c.maxLifetime {
		return false
	}
	if c.connMaxIdleTime > 0 && time.Since(c.usedAt) > c.connMaxIdleTime {
		return false
	}
	return true
}

func ConnUseAt(conn Connection) error {
	c := conn.(*ConnectionImpl)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.usedAt = time.Now()
	c.inUsed = true
	// log.Printf("conn is used at:%v\n", c.usedAt)
	return nil
}
