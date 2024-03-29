package goloadbalancer

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	c "github.com/smartystreets/goconvey/convey"
) // 别名导入

func testServer() {
	listen, err := net.Listen("tcp", "127.0.0.1:9090")
	if err != nil {
		log.Fatalf("listen failed, err:%v\n", err)
		return
	}
	for {
		// 等待客户端建立连接
		conn, err := listen.Accept()
		if err != nil {
			log.Printf("accept failed, err:%v\n", err)
			continue
		}
		// 启动一个单独的 goroutine 去处理连接
		go func(c net.Conn) {
			defer conn.Close()
			for {
				reader := bufio.NewReader(conn)
				var buf [256]byte
				n, err := reader.Read(buf[:])
				if err != nil && err.Error() != "EOF" {
					fmt.Printf("read from conn failed, err:%v\n", err)
					break
				}
				if err == io.EOF {
					break
				}
				recv := string(buf[:n])
				log.Printf("接收到的数据：%v", recv)
				// 将接受到的数据返回给客户端
				_, err = conn.Write([]byte("ok"))
				if err != nil {
					log.Printf("write to client from conn failed, err:%v\n", err)
					break
				}
			}
		}(conn)
	}
}
func TestPool(t *testing.T) {
	c.Convey("TestPool", t, func() {
		go testServer()
		time.Sleep(1 * time.Second)
		opts := NewPoolOptions(func(ctx context.Context) (Connection, error) {
			conn, err := net.Dial("tcp", "127.0.0.1:9090")
			if err != nil {
				return nil, err
			}
			return NewConnectionImpl(conn, 30*time.Second, 10*time.Second), nil
		})
		opts.ConnectionUsedHook = append(opts.ConnectionUsedHook, ConnUseAt)
		pool := NewConnPool(opts)
		wg := &sync.WaitGroup{}
		shouldBeRemoved := make([]string, 0)
		lock := sync.RWMutex{}
		errChan := make(chan error, 10)
		elapsedChan := make(chan float64, 10)
		IdleConnsChan := make(chan uint32, 10)
		connChanCount := make(chan uint32, 10)
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx := context.Background()
				// 初始化随机数种子
				src := rand.NewSource(time.Now().UnixNano())
				r := rand.New(src)

				// 获取3到10之间的随机数
				randomNumber := r.Intn(8) + 2
				time.Sleep(time.Duration(randomNumber) * time.Second)
				now := time.Now()

				cn, err := pool.Get(ctx)
				if err != nil {
					elapsed := time.Since(now)
					errChan <- err
					elapsedChan <- elapsed.Seconds()
					IdleConnsChan <- pool.Stats().(*StatsImpl).IdleConns
					connChanCount <- pool.Stats().(*StatsImpl).TotalConns
					if errors.Is(err, ErrPoolTimeout) {
						log.Printf("get conn failed, err:%v,%f,%d\n", err, elapsed.Seconds(), pool.Stats().(*StatsImpl).IdleConns)

					}
					return
				}
				randomNumber = r.Intn(8) + 4
				time.Sleep(time.Duration(randomNumber) * time.Second)
				// log.Printf("total connections:%d\n", Stats().TotalConns)

				conn := cn.ConnInstance().(net.Conn)
				if randomNumber*int(time.Second) > 10*int(time.Second) {
					lock.Lock()
					shouldBeRemoved = append(shouldBeRemoved, conn.LocalAddr().String())
					lock.Unlock()
				}
				defer pool.Release(ctx, cn)
				_, err = conn.Write([]byte("hello"))
				if err != nil {
					log.Printf("write to server failed, err:%v\n", err)
					return
				}
				var buf [1024]byte
				_, err = conn.Read(buf[:])
				if err != nil && err.Error() != "EOF" {
					fmt.Printf("read failed, err:%v\n", err.Error() == "EOF")
					return
				}
			}()
		}

		wg.Wait()
		close(errChan)
		close(elapsedChan)
		close(IdleConnsChan)
		close(connChanCount)
		for err := range errChan {
			c.So(err, c.ShouldEqual, ErrPoolTimeout)
		}
		for elapsed := range elapsedChan {
			c.So(elapsed, c.ShouldBeBetween, 6, 7)

		}
		for idleConns := range IdleConnsChan {
			c.So(idleConns, c.ShouldEqual, 0)

		}
		for connCount := range connChanCount {
			c.So(connCount, c.ShouldEqual, opts.PoolSize)

		}

		c.So(pool.Stats().(*StatsImpl).InvalidConns, c.ShouldBeGreaterThanOrEqualTo, len(shouldBeRemoved))

		// log.Printf("total REMOVE connections:%d\n", Stats().IdleConns)
	})
}
