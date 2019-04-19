package redis4g

import (
	"fmt"
)

import (
	"github.com/go-redis/redis"
)

// 定义redis连接
type WrapClient struct {
	conf       *ConnectionParameter
	connection *redis.Client
}

func (c *WrapClient) TransformKey(k string) string {
	return c.conf.Prefix + ":" + k

}

func (c *WrapClient) TransformKeyList(k ...string) []string {
	mk := make([]string, len(k))
	for i, strK := range k {
		mk[i] = c.TransformKey(strK)
	}
	return mk
}

func (c *WrapClient) Config() ConnectionParameter {
	return *c.conf
}

func (c *WrapClient) Conn() *redis.Client {
	return c.connection
}

func (c *WrapClient) Close() {
	if c.connection != nil {
		_ = c.connection.Close()
		c.connection = nil
	}
}

func (c *WrapClient) tryConnect() error {

	var reOpen = false
	if c.connection == nil {
		reOpen = true
	} else {
		if st := c.connection.Ping(); st == nil || st.Err() != nil {
			reOpen = true
		}
	}

	if reOpen {
		conn := redis.NewClient(&redis.Options{
			DB:       c.conf.Db,
			Addr:     fmt.Sprintf("%v:%v", c.conf.Host, c.conf.Port),
			Password: c.conf.Password,
			PoolSize: c.conf.MaxConnections,
		})
		if err := conn.Ping().Err(); err != nil {
			return err
		}

		if c.connection != nil {
			_ = c.connection.Close()
		}
		c.connection = conn
	}
	return nil
}
