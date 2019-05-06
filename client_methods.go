package redis4g

import (
	"time"
)

import (
	"github.com/go-redis/redis"
)

func (c *WrapClient) Get(k string) string {
	var s string
	s, _ = c.connection.Get(c.TransformKey(k)).Result()
	return s

}

func (c *WrapClient) GetInt64(k string) int64 {
	var i int64
	i, _ = c.connection.Get(c.TransformKey(k)).Int64()
	return i
}

func (c *WrapClient) GetFloat64(k string) float64 {
	var f float64
	f, _ = c.connection.Get(c.TransformKey(k)).Float64()
	return f

}

func (c *WrapClient) Set(k string, v interface{}) bool {
	return c.SetEx(k, v, 0)
}

func (c *WrapClient) SetEx(k string, v interface{}, ex time.Duration) bool {
	s, _ := c.connection.Set(c.TransformKey(k), v, ex).Result()
	if s == "OK" || s == "ok" {
		return true
	}
	return false
}

func (c *WrapClient) SetNx(k string, v interface{}, ex time.Duration) bool {
	b, _ := c.connection.SetNX(c.TransformKey(k), v, ex).Result()
	return b
}

func (c *WrapClient) MSet(data map[string]interface{}) bool {

	var pairs = make([]interface{}, 2*len(data))
	var idx = 0

	for k, v := range data {
		pairs[2*idx] = c.TransformKey(k)
		pairs[2*idx+1] = v
		idx++
	}

	s, _ := c.connection.MSet(pairs...).Result()
	if s == "OK" || s == "ok" {
		return true
	}
	return false
}

func (c *WrapClient) MGet(k ...string) []interface{} {
	mk := c.TransformKeyList(k...)
	r, _ := c.connection.MGet(mk...).Result()
	return r
}

func (c *WrapClient) LLen(k string) int64 {

	i, _ := c.connection.LLen(c.TransformKey(k)).Result()
	return i
}

func (c *WrapClient) LIndex(k string, i int64) string {
	r, _ := c.connection.LIndex(c.TransformKey(k), i).Result()
	return r
}

func (c *WrapClient) LRange(k string, start, end int64) []string {
	r, _ := c.connection.LRange(c.TransformKey(k), start, end).Result()
	return r
}

func (c *WrapClient) LPush(k string, values ...interface{}) int64 {
	r, _ := c.connection.LPush(c.TransformKey(k), values...).Result()
	return r
}

func (c *WrapClient) RPush(k string, values ...interface{}) int64 {
	r, _ := c.connection.RPush(c.TransformKey(k), values...).Result()
	return r
}

func (c *WrapClient) LPop(k string) string {
	r, _ := c.connection.LPop(c.TransformKey(k)).Result()
	return r
}

func (c *WrapClient) RPop(k string) string {
	r, _ := c.connection.RPop(c.TransformKey(k)).Result()
	return r
}

func (c *WrapClient) HGetAll(k string) map[string]string {
	r, _ := c.connection.HGetAll(c.TransformKey(k)).Result()
	return r
}

func (c *WrapClient) HGet(k, fieldName string) string {
	r, _ := c.connection.HGet(c.TransformKey(k), fieldName).Result()
	return r
}

func (c *WrapClient) HSet(k, fieldName string, value interface{}) bool {
	if _, err := c.connection.HSet(c.TransformKey(k), fieldName, value).Result(); err != nil {
		return false
	}
	return true
}

func (c *WrapClient) HMGet(k string, fields ...string) []interface{} {
	r, _ := c.connection.HMGet(c.TransformKey(k), fields...).Result()
	return r
}

func (c *WrapClient) HMSet(k string, values map[string]interface{}) bool {
	r, _ := c.connection.HMSet(c.TransformKey(k), values).Result()
	if r == "OK" || r == "ok" {
		return true
	}
	return false
}

func (c *WrapClient) HDel(k, fieldName string) int64 {
	r, _ := c.connection.HDel(c.TransformKey(k), fieldName).Result()
	return r
}

func (c *WrapClient) HExists(k, fieldName string) bool {
	r, _ := c.connection.HExists(c.TransformKey(k), fieldName).Result()
	return r

}

func (c *WrapClient) HLen(k string) int64 {
	r, _ := c.connection.HLen(c.TransformKey(k)).Result()
	return r
}

func (c *WrapClient) Incr(k string) int64 {
	r, _ := c.connection.IncrBy(c.TransformKey(k), 1).Result()
	return r
}

func (c *WrapClient) Decr(k string) int64 {
	r, _ := c.connection.DecrBy(c.TransformKey(k), 1).Result()
	return r
}

func (c *WrapClient) Exists(k ...string) int64 {
	mk := c.TransformKeyList(k...)
	i, _ := c.connection.Exists(mk...).Result()
	return i
}

func (c *WrapClient) Delete(k ...string) int64 {
	mk := c.TransformKeyList(k...)
	i, _ := c.connection.Del(mk...).Result()
	return i
}

func (c *WrapClient) Keys(k string) []string {
	ret, _ := c.connection.Keys(c.TransformKey(k)).Result()
	return ret
}

func (c *WrapClient) SAdd(k string, value string) int64 {
	i, _ := c.connection.SAdd(c.TransformKey(k), value).Result()
	return i
}

func (c *WrapClient) SMembers(k string) []string {
	r, _ := c.connection.SMembers(c.TransformKey(k)).Result()
	return r
}

func (c *WrapClient) SRem(k string, value string) int64 {
	r, _ := c.connection.SRem(c.TransformKey(k), value).Result()
	return r
}

func (c *WrapClient) Pipeline() redis.Pipeliner {
	p := c.connection.Pipeline()
	return p
}
