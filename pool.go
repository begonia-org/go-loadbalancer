package goloadbalancer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrClosed performs any operation on the closed client will return this error.
	ErrClosed = errors.New("connection is closed")

	// ErrPoolExhausted is returned from a pool connection method
	// when the maximum number of database connections in the pool has been reached.
	ErrPoolExhausted = errors.New("connection pool exhausted")

	// ErrPoolTimeout timed out waiting to get a connection from the connection pool.
	ErrPoolTimeout = errors.New("connection pool timeout")

	ErrBadConn = errors.New("bad connection")
)

var timers = sync.Pool{
	New: func() interface{} {
		t := time.NewTimer(time.Hour)
		t.Stop()
		return t
	},
}

type Connection interface {
	Close() error
	Validate() bool
	ConnInstance() Conn
	IsUsing() bool
	InUsed(bool)
}
type Pool interface {
	// NewConn(context.Context) (Connection, error)
	CloseConn(Connection) error

	Get(context.Context) (Connection, error)
	Release(context.Context, Connection)
	Remove(context.Context, Connection, error) error

	Len() int
	IdleLen() int32
	Stats() Stats

	Close() error
}
type ConnUsedHook func(Connection) error

type PoolOptions struct {
	Dialer             func(context.Context) (Connection, error)
	ConnectionUsedHook []ConnUsedHook
	PoolFIFO           bool
	PoolSize           int32
	PoolTimeout        time.Duration
	MinIdleConns       int32
	MaxIdleConns       int32
	MaxActiveConns     int32
}
type PoolOptionsBuildOption func(*PoolOptions)

type lastDialErrorWrap struct {
	err error
}

type ConnPool struct {
	cfg *PoolOptions

	dialErrorsNum uint32 // atomic
	lastDialError atomic.Value

	queue chan struct{}

	connsMu sync.Mutex
	// 存储所有的连接，包括空闲和正在使用的
	conns []Connection
	// 存储空闲的连接
	idleConns []Connection
	// 连接池的大小
	poolSize int32
	// 空闲连接的数量
	idleConnsLen int32

	stats *StatsImpl

	_closed uint32 // atomic

	connCreator *sync.Pool
}

// WithPoolSize 设置连接池的大小
func WithPoolSize(size int32) PoolOptionsBuildOption {
	return func(o *PoolOptions) {
		o.PoolSize = size
	}
}

// WithPoolTimeout 设置排队等待连接的超时时间
func WithPoolTimeout(timeout time.Duration) PoolOptionsBuildOption {
	return func(o *PoolOptions) {
		o.PoolTimeout = timeout
	}
}

// WithMinIdleConns 设置连接池的最小空闲连接数
func WithMinIdleConns(minIdleConns int32) PoolOptionsBuildOption {
	return func(o *PoolOptions) {
		o.MinIdleConns = minIdleConns
	}
}

// WithMaxIdleConns 设置连接池的最大空闲连接数
func WithMaxIdleConns(maxIdleConns int32) PoolOptionsBuildOption {
	return func(o *PoolOptions) {
		o.MaxIdleConns = maxIdleConns
	}
}

// WithMaxActiveConns 设置连接池的最大活跃连接数
func WithMaxActiveConns(maxActiveConns int32) PoolOptionsBuildOption {
	return func(o *PoolOptions) {
		o.MaxActiveConns = maxActiveConns
	}
}

// WithConnectionUsedHook 设置连接使用的钩子函数
func WithConnectionUsedHook(hook ...ConnUsedHook) PoolOptionsBuildOption {
	return func(o *PoolOptions) {
		o.ConnectionUsedHook = append(o.ConnectionUsedHook, hook...)
	}

}

// WithDialer 设置连接池的拨号函数
func WithDialer(dialer func(context.Context) (Connection, error)) PoolOptionsBuildOption {
	return func(o *PoolOptions) {
		o.Dialer = dialer
	}

}
func NewPoolOptions(Dialer func(context.Context) (Connection, error), opts ...PoolOptionsBuildOption) *PoolOptions {
	options := &PoolOptions{
		ConnectionUsedHook: make([]ConnUsedHook, 0),
		Dialer:             Dialer,
		PoolSize:           20,
		PoolTimeout:        6 * time.Second,
		MinIdleConns:       3,
		MaxIdleConns:       6,
		MaxActiveConns:     10,
	}
	for _, opt := range opts {
		opt(options)

	}
	return options

}

func NewConnPool(opt *PoolOptions) Pool {
	p := &ConnPool{
		cfg: opt,
		// poolSize:  opt.PoolSize,
		queue:     make(chan struct{}, opt.PoolSize),
		conns:     make([]Connection, 0, opt.PoolSize),
		idleConns: make([]Connection, 0, opt.PoolSize),
		stats:     &StatsImpl{},
		connCreator: &sync.Pool{
			New: func() interface{} {
				conn, err := opt.Dialer(context.Background())
				if err != nil {
					return err
				}
				return conn
			},
		},
	}

	// p.connsMu.Lock()
	p.checkMinIdleConns()
	// defer p.connsMu.Unlock()

	return p
}

// checkMinIdleConns 检查连接池内的连接数量是否达到最小空闲连接数
// 如果没有达到最小空闲连接数，就创建新的连接
// 如果达到了最小空闲连接数，就不创建新的连接
func (p *ConnPool) checkMinIdleConns() {
	if p.cfg.MinIdleConns == 0 {
		return
	}
	// 填充空闲连接
	for p.poolSize < p.cfg.PoolSize && p.idleConnsLen < p.cfg.MinIdleConns {
		select {
		case p.queue <- struct{}{}:
			atomic.AddInt32(&p.poolSize, 1)
			atomic.AddInt32(&p.idleConnsLen, 1)

			go func() {
				err := p.addIdleConn()
				if err != nil && err != ErrClosed {

					atomic.AddInt32(&p.poolSize, -1)
					atomic.AddInt32(&p.idleConnsLen, -1)

				}

				p.freeTurn()
			}()
		default:
			return
		}
	}
}

// addIdleConn 添加一个空闲连接
func (p *ConnPool) addIdleConn() error {
	cn, err := p.dialConn(context.TODO())
	if err != nil {
		return err
	}

	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	// It is not allowed to add new connections to the closed connection pool.
	if p.closed() {
		_ = cn.Close()
		return ErrClosed
	}

	p.conns = append(p.conns, cn)
	p.idleConns = append(p.idleConns, cn)
	return nil
}

func (p *ConnPool) newConn(ctx context.Context) (Connection, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	// 连接池已经满了
	if p.cfg.MaxActiveConns > 0 && p.poolSize >= p.cfg.MaxActiveConns {
		return nil, ErrPoolExhausted
	}

	cn, err := p.dialConn(ctx)
	if err != nil {
		return nil, err
	}

	// 连接池已经满了
	if p.cfg.MaxActiveConns > 0 && p.poolSize >= p.cfg.MaxActiveConns {
		_ = cn.Close()
		return nil, ErrPoolExhausted
	}

	p.conns = append(p.conns, cn)
	p.poolSize++

	return cn, nil
}
func (p *ConnPool) dialer() (Connection, error) {
	//  newConn:=p.connCreator.Get()
	//  if err,ok:=newConn.(error);ok{
	//  	return nil,err
	//  }
	//  return newConn.(Connection),nil
	return p.cfg.Dialer(context.Background())
}
func (p *ConnPool) dialConn(_ context.Context) (Connection, error) {
	if p.closed() {
		return nil, ErrClosed
	}

	if atomic.LoadUint32(&p.dialErrorsNum) >= uint32(p.cfg.PoolSize) {
		return nil, p.getLastDialError()
	}
	// 构建新的连接
	netConn, err := p.dialer()
	if err != nil {
		p.setLastDialError(err)
		if atomic.AddUint32(&p.dialErrorsNum, 1) == uint32(p.cfg.PoolSize) {
			go p.tryDial()
		}
		return nil, err
	}

	return netConn, nil
}

func (p *ConnPool) tryDial() {
	for {
		if p.closed() {
			return
		}

		conn, err := p.dialer()
		if err != nil {
			p.setLastDialError(err)
			time.Sleep(time.Second)
			continue
		}

		atomic.StoreUint32(&p.dialErrorsNum, 0)
		_ = conn.Close()
		return
	}
}

func (p *ConnPool) setLastDialError(err error) {
	p.lastDialError.Store(&lastDialErrorWrap{err: err})
}

func (p *ConnPool) getLastDialError() error {
	err, _ := p.lastDialError.Load().(*lastDialErrorWrap)
	if err != nil {
		return err.err
	}
	return nil
}

// Get 获取或创建一个连接
func (p *ConnPool) Get(ctx context.Context) (Connection, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	// 排队等待获取连接的机会
	if err := p.waitTurn(ctx); err != nil {
		return nil, err
	}

	for {
		p.connsMu.Lock()
		cn, err := p.popIdle()
		p.connsMu.Unlock()

		if err != nil {
			p.freeTurn()
			return nil, err
		}

		if cn == nil {
			break
		}

		// 检查连接是否有效
		if !cn.Validate() {
			atomic.AddUint32(&p.stats.InvalidConns, 1)
			_ = p.CloseConn(cn)
			continue
		}

		atomic.AddUint32(&p.stats.Hits, 1)
		for _, hook := range p.cfg.ConnectionUsedHook {
			if err := hook(cn); err != nil {
				return nil, err
			}
		}
		atomic.AddInt32(&p.stats.InUsedConns, 1)
		cn.InUsed(true)
		return cn, nil
	}

	atomic.AddUint32(&p.stats.Misses, 1)

	newcn, err := p.newConn(ctx)
	if err != nil {
		p.freeTurn()
		return nil, err
	}
	atomic.AddInt32(&p.stats.InUsedConns, 1)
	newcn.InUsed(true)
	return newcn, nil
}

// waitTurn 排队等待获取连接的机会
// 如果上下文被取消，会清理定时器并返回上下文的错误。
// 如果能成功将空结构体发送到p.queue通道中，表示成功排队，会清理定时器并返回nil。
// 如果定时器超时，表示在配置的等待时间内未能获取到连接，会增加超时统计并返回ErrPoolTimeout错误。
func (p *ConnPool) waitTurn(ctx context.Context) error {
	// 检查请求是否已经取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case p.queue <- struct{}{}:
		return nil
	default:
	}

	timer := timers.Get().(*time.Timer)
	timer.Reset(p.cfg.PoolTimeout)

	select {
	case <-ctx.Done():
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return ctx.Err()
	// 排队成功，可以获取到连接
	case p.queue <- struct{}{}:
		if !timer.Stop() {
			<-timer.C
		}
		timers.Put(timer)
		return nil
	case <-timer.C:

		timers.Put(timer)
		atomic.AddUint32(&p.stats.Timeouts, 1)
		return ErrPoolTimeout
	}
}

// freeTurn 释放一个排队的机会
func (p *ConnPool) freeTurn() {
	<-p.queue
}

// popIdle 从空闲连接中弹出一个连接
func (p *ConnPool) popIdle() (Connection, error) {
	if p.closed() {
		return nil, ErrClosed
	}
	n := len(p.idleConns)
	if n == 0 {
		return nil, nil
	}

	var cn Connection
	if p.cfg.PoolFIFO {
		cn = p.idleConns[0]
		copy(p.idleConns, p.idleConns[1:])
		p.idleConns = p.idleConns[:n-1]
	} else {
		idx := n - 1
		cn = p.idleConns[idx]
		p.idleConns = p.idleConns[:idx]
	}
	atomic.AddInt32(&p.idleConnsLen, -1)
	p.checkMinIdleConns()
	return cn, nil
}

// Release 将连接放回连接池
func (p *ConnPool) Release(ctx context.Context, cn Connection) {
	if cn == nil {
		return
	}
	defer func() {
		atomic.AddInt32(&p.stats.InUsedConns, -1)
		p.freeTurn()

	}()
	if !cn.Validate() {
		atomic.AddUint32(&p.stats.InvalidConns, 1)
		cn.InUsed(false)
		_ = p.Remove(ctx, cn, ErrBadConn)
		return
	}

	var shouldCloseConn bool

	p.connsMu.Lock()
	// 检查连接池的数据量关系
	if p.cfg.MaxIdleConns == 0 || p.idleConnsLen < p.cfg.MaxIdleConns {
		p.idleConns = append(p.idleConns, cn)
		p.idleConnsLen++
	} else {
		// 如果空闲连接的数量已经达到了最大空闲连接数，就关闭这个连接
		p.removeConn(cn)
		shouldCloseConn = true
	}
	defer p.connsMu.Unlock()

	if shouldCloseConn {
		_ = p.closeConn(cn)
	}
	cn.InUsed(false)
}

func (p *ConnPool) Remove(_ context.Context, cn Connection, reason error) error {
	err := p.removeConnWithLock(cn)
	if err != nil {
		return err
	}
	p.freeTurn()
	return p.closeConn(cn)
}

func (p *ConnPool) CloseConn(cn Connection) error {

	err := p.removeConnWithLock(cn)
	if err != nil {
		return err
	}
	return p.closeConn(cn)
}

func (p *ConnPool) removeConnWithLock(cn Connection) error {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	if cn.IsUsing() {
		return fmt.Errorf("connection is using, can't close")
	}
	p.removeConn(cn)
	return nil
}

// removeConn 从连接池中移除一个连接
func (p *ConnPool) removeConn(cn Connection) {
	for i, c := range p.conns {
		if c == cn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			atomic.AddInt32(&p.poolSize, -1)
			p.checkMinIdleConns()
			break
		}
	}
	atomic.AddUint32(&p.stats.StaleConns, 1)
}

// CloseConn 关闭连接
func (p *ConnPool) closeConn(cn Connection) error {
	if cn.IsUsing() {
		return fmt.Errorf("connection is using, can't close")
	}

	// p.connCreator.Put(cn)
	return cn.Close()
}

// Len returns total number of connections.
func (p *ConnPool) Len() int {
	p.connsMu.Lock()
	n := len(p.conns)
	p.connsMu.Unlock()
	return n
}

// IdleLen returns number of idle connections.
func (p *ConnPool) IdleLen() int32 {
	p.connsMu.Lock()
	n := p.idleConnsLen
	p.connsMu.Unlock()
	return n
}

func (p *ConnPool) Stats() Stats {
	return &StatsImpl{
		Hits:     atomic.LoadUint32(&p.stats.Hits),
		Misses:   atomic.LoadUint32(&p.stats.Misses),
		Timeouts: atomic.LoadUint32(&p.stats.Timeouts),

		TotalConns:   uint32(p.Len()),
		IdleConns:    uint32(p.IdleLen()),
		StaleConns:   atomic.LoadUint32(&p.stats.StaleConns),
		InvalidConns: atomic.LoadUint32(&p.stats.InvalidConns),
		InUsedConns:  atomic.LoadInt32(&p.stats.InUsedConns),
	}
}

func (p *ConnPool) closed() bool {
	return atomic.LoadUint32(&p._closed) == 1
}

// Close 关闭连接池
func (p *ConnPool) Close() error {
	if !atomic.CompareAndSwapUint32(&p._closed, 0, 1) {
		return ErrClosed
	}

	var firstErr error
	p.connsMu.Lock()
	for _, cn := range p.conns {
		if err := p.closeConn(cn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.conns = nil
	p.poolSize = 0
	p.idleConns = nil
	p.idleConnsLen = 0
	defer p.connsMu.Unlock()

	return firstErr
}
