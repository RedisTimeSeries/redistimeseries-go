package redis_timeseries

import (
	"math/rand"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

type ConnPool interface {
	Get() redis.Conn
}

type SingleHostPool struct {
	*redis.Pool
}

func NewSingleHostPool(host string) *SingleHostPool {
	pool := &redis.Pool{
		// Other pool configuration not shown in this example.
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", host)
			if err != nil {
				return nil, err
			}
			return c, nil
		},
	}
	pool.TestOnBorrow = func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > time.Second {
			_, err = c.Do("PING")
		}
		return err
	}
	return &SingleHostPool{pool}
}

type MultiHostPool struct {
	sync.Mutex
	pools map[string]*redis.Pool
	hosts []string
}

func NewMultiHostPool(hosts []string) *MultiHostPool {

	return &MultiHostPool{
		pools: make(map[string]*redis.Pool, len(hosts)),
		hosts: hosts,
	}
}

func (p *MultiHostPool) Get() redis.Conn {
	p.Lock()
	defer p.Unlock()
	host := p.hosts[rand.Intn(len(p.hosts))]
	pool, found := p.pools[host]
	if !found {
		pool := &redis.Pool{
			// Other pool configuration not shown in this example.
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", host)
				if err != nil {
					return nil, err
				}
				return c, nil
			},
		}
		pool.TestOnBorrow = func(c redis.Conn, t time.Time) error {
			if time.Since(t).Seconds() > 1 {
				_, err := c.Do("PING")
				return err
			}
			return nil
		}

		p.pools[host] = pool
	}
	return pool.Get()

}
