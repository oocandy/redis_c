package redis_c

import (
	"fmt"

	"github.com/garyburd/redigo/redis"
)

type (
	//双向队列
	IList interface {
		Key() string                                                          // 返回List的Key
		Clear() (bool, error)                                                 // 清空整个队列
		Count() (int, error)                                                  // 获取Item个数
		PopFront() (interface{}, error)                                       // 弹出队列第一个元素
		PopBack() (interface{}, error)                                        // 弹出队列最后一个元素
		PopFrontWait(timeoutSec int) (interface{}, error)                     // 弹出队列第一个元素, 若列表为空则等待(超时为0表示无限期等待)
		PopBackWait(timeoutSec int) (interface{}, error)                      // 弹出队列最后一个元素, 若列表为空则等待(超时为0表示无限期等待)
		PushFront(values ...interface{}) error                                // 从队列最前面压入元素(若有多个项, 则第一项先压入)
		PushBack(values ...interface{}) error                                 // 从队列最后面压入元素(若有多个项, 则第一项先压入)
		Get(index int) (interface{}, error)                                   // 通过获取索引获取元素(队列索引从0开始)
		Set(index int, value interface{}) error                               // 通过索引设置元素的值
		Remove(value interface{}) (int, error)                                // 返回被移除的Item个数
		PopBackToFront(toList IList) (interface{}, error)                     // 弹出队列最后一个元素并压入另一个List前部, 返回值为操作的元素
		PopBackToFrontWait(toList IList, timeoutSec int) (interface{}, error) // 弹出队列最后一个元素并压入另一个List前部(队列中无元素时等待)返回值为操作的元素(超时为0表示无限期等待)
		Trim(start, stop int) error                                           // 只保留Start-Stop之间的元素,其他的删除, 下标 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
		Range(start, stop int) ([]interface{}, error)                         // 返回Start-Stop之间的元素, 可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	}
)

type List struct {
	_key  string
	_pool *redis.Pool
}

func (s *List) Key() string {
	return s._key
}

func (s *List) Clear() (bool, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("DEL", redis.Args{}.Add(s._key)...))
}

func (s *List) Count() (int, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("LLEN", redis.Args{}.Add(s._key)...))
}

func (s *List) PopFront() (interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return conn.Do("LPOP", redis.Args{}.Add(s._key)...)
}

func (s *List) PopBack() (interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return conn.Do("RPOP", redis.Args{}.Add(s._key)...)
}

func (s *List) PopFrontWait(timeoutSec int) (interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	rets, err := redis.Values(conn.Do("BLPOP", redis.Args{}.Add(s._key).Add(timeoutSec)...))
	if err != nil {
		return nil, err
	}
	if len(rets) == 2 && rets[0] != nil {
		return rets[1], nil
	}
	return nil, fmt.Errorf("error return format")
}

func (s *List) PopBackWait(timeoutSec int) (interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	rets, err := redis.Values(conn.Do("BRPOP", redis.Args{}.Add(s._key).Add(timeoutSec)...))
	if err != nil {
		return nil, err
	}
	if len(rets) == 2 && rets[0] != nil {
		return rets[1], nil
	}
	return nil, fmt.Errorf("error return format")
}

func (s *List) PushFront(values ...interface{}) error {
	conn := s._pool.Get()
	defer conn.Close()
	_, err := redis.Bool(conn.Do("LPUSH", redis.Args{}.Add(s._key).AddFlat(values)...))
	return err
}

func (s *List) PushBack(values ...interface{}) error {
	conn := s._pool.Get()
	defer conn.Close()
	_, err := redis.Bool(conn.Do("RPUSH", redis.Args{}.Add(s._key).AddFlat(values)...))
	return err
}

func (s *List) Get(index int) (interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return conn.Do("LINDEX", redis.Args{}.Add(s._key).Add(index)...)
}

func (s *List) Set(index int, value interface{}) error {
	conn := s._pool.Get()
	defer conn.Close()
	_, err := conn.Do("LSET", redis.Args{}.Add(s._key).Add(index).Add(value)...)
	return err
}

func (s *List) Remove(value interface{}) (int, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("LREM", redis.Args{}.Add(s._key).Add(0).Add(value)...))
}

func (s *List) PopBackToFront(toList IList) (interface{}, error) {
	if toList == nil {
		return nil, fmt.Errorf("toList is nil")
	}
	toKey := toList.Key()
	if toKey == "" {
		return nil, fmt.Errorf("toList.Key() is empty")
	}
	conn := s._pool.Get()
	defer conn.Close()
	return conn.Do("RPOPLPUSH", redis.Args{}.Add(s._key).Add(toKey)...)
}

func (s *List) PopBackToFrontWait(toList IList, timeoutSec int) (interface{}, error) {
	if toList == nil {
		return nil, fmt.Errorf("toList is nil")
	}
	toKey := toList.Key()
	if toKey == "" {
		return nil, fmt.Errorf("toList.Key() is empty")
	}
	if timeoutSec < 0 {
		return nil, fmt.Errorf("timeoutSec is lessthan zero")
	}
	conn := s._pool.Get()
	defer conn.Close()
	return conn.Do("BRPOPLPUSH", redis.Args{}.Add(s._key).Add(toKey).Add(timeoutSec)...)
}

func (s *List) Trim(start, stop int) error {
	conn := s._pool.Get()
	defer conn.Close()
	_, err := conn.Do("LTRIM", redis.Args{}.Add(s._key).Add(start).Add(stop)...)
	return err
}

func (s *List) Range(start, stop int) ([]interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Values(conn.Do("LRANGE", redis.Args{}.Add(s._key).Add(start).Add(stop)...))
}

func NewList(key string, pool *redis.Pool) IList {
	return &List{
		_key:  key,
		_pool: pool,
	}
}
