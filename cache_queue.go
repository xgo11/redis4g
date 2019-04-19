package redis4g

import (
	"sync"
)

type Queue interface {
	GetName() string
	Size() int64
	Push(...interface{}) bool
	Pop() string
	Close()
}

// 定义redis队列
type queueImpl struct {
	sync.Mutex

	name   string
	conf   string
	client *WrapClient
}

// 获取队列名称
func (q *queueImpl) GetName() string {
	return q.name
}

// 获取队列当前数据量
func (q *queueImpl) Size() int64 {
	q.Lock()
	defer q.Unlock()
	var size = q.client.LLen(q.name)
	return size
}

// 向队列推送数据
func (q *queueImpl) Push(v ...interface{}) bool {
	q.Lock()
	defer q.Unlock()
	cnt := q.client.LPush(q.name, v...)
	if cnt > 0 {
		return true
	}
	return false
}

// 从队列读取一条数据
func (q *queueImpl) Pop() string {
	q.Lock()
	defer q.Unlock()
	result := q.client.RPop(q.name)
	return result

}

// 关闭队列
func (q *queueImpl) Close() {
	if q.client != nil {
		q.client.Close()
	}
}
