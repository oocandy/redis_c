package redis_c

import (
	"github.com/garyburd/redigo/redis"
)

//redis.Pool
//HINCRBY

type (
	IHash interface {
		Key() string                                                  // 返回本Hash表的KEy
		Clear() (bool, error)                                         // 清空整个Hash表
		Dels(names ...interface{}) (int, error)                       // 删除Names
		Del(name interface{}) (bool, error)                           // 删除Name
		Has(name interface{}) (bool, error)                           // 是否存在Name
		Set(name, value interface{}) error                            // 设置Name:VALUE
		SetNX(name, value interface{}) (bool, error)                  // 当Name不存在时,才设置VALUE
		Sets(pairs map[interface{}]interface{}) error                 // 设置一组 设置Name/VALUE
		Get(name interface{}) (interface{}, error)                    // 获取VALUE
		Gets(names ...interface{}) ([]interface{}, error)             // 获取[]VALUE
		Names() ([]interface{}, error)                                // 获取所有的[]Name
		Values() ([]interface{}, error)                               // 获取所有的[]Value
		Pairs() (interface{}, error)                                  // 获取所有的键值对(需要自己转换)
		Count() (int, error)                                          // 获取 Name个数
		IncInt(name interface{}, amount int) (int, error)             // 给整数值增加一个值(返回增加后的值)
		IncInt64(name interface{}, amount int64) (int64, error)       // 给整数值增加一个值(返回增加后的值)
		IncUint64(name interface{}, amount uint64) (uint64, error)    // 给整数值增加一个值(返回增加后的值)
		IncFloat64(name interface{}, amount float64) (float64, error) // 给浮点数64值增加一个值(返回增加后的值)
	}
)

type Hash struct {
	_key  string
	_pool *redis.Pool
}

func NewHash(key string, pool *redis.Pool) IHash {
	return &Hash{
		_key:  key,
		_pool: pool,
	}
}

func (s *Hash) Key() string {
	return s._key
}

func (s *Hash) Clear() (bool, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("DEL", redis.Args{}.Add(s._key)...))
}

func (s *Hash) Dels(names ...interface{}) (int, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("HDEL", redis.Args{}.Add(s._key).AddFlat(names)...))
}

func (s *Hash) Del(name interface{}) (bool, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("HDEL", redis.Args{}.Add(s._key).Add(name)...))
}

func (s *Hash) Has(name interface{}) (bool, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("HEXISTS", redis.Args{}.Add(s._key).Add(name)...))
}

func (s *Hash) Set(name, value interface{}) error {
	conn := s._pool.Get()
	defer conn.Close()
	_, err := redis.Bool(conn.Do("HSET", redis.Args{}.Add(s._key).Add(name).Add(value)...))
	return err
}

func (s *Hash) SetNX(name, value interface{}) (bool, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Bool(conn.Do("HSETNX", redis.Args{}.Add(s._key).Add(name).Add(value)...))
}

func (s *Hash) Sets(pairs map[interface{}]interface{}) error {
	conn := s._pool.Get()
	defer conn.Close()
	args := redis.Args{s._key}
	for k, v := range pairs {
		args = args.Add(k).Add(v)
	}
	if _, err := conn.Do("HMSET", args...); err != nil {
		return err
	}
	return nil
}

func (s *Hash) Get(name interface{}) (interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return conn.Do("HGET", redis.Args{}.Add(s._key).Add(name)...)
}

func (s *Hash) Gets(names ...interface{}) ([]interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Values(conn.Do("HMGET", redis.Args{}.Add(s._key).AddFlat(names)...))
}

func (s *Hash) Names() ([]interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Values(conn.Do("HKEYS", redis.Args{}.Add(s._key)...))
}

func (s *Hash) Values() ([]interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Values(conn.Do("HVALS", redis.Args{}.Add(s._key)...))
}

func (s *Hash) Pairs() (interface{}, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return conn.Do("HGETALL", redis.Args{}.Add(s._key)...)
}

func (s *Hash) Count() (int, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("HLEN", redis.Args{}.Add(s._key)...))
}

func (s *Hash) IncInt(name interface{}, amount int) (int, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("HINCRBY", redis.Args{}.Add(s._key)...))
}

func (s *Hash) IncInt64(name interface{}, amount int64) (int64, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Int64(conn.Do("HINCRBY", redis.Args{}.Add(s._key)...))
}

func (s *Hash) IncUint64(name interface{}, amount uint64) (uint64, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Uint64(conn.Do("HINCRBY", redis.Args{}.Add(s._key)...))
}

func (s *Hash) IncFloat64(name interface{}, amount float64) (float64, error) {
	conn := s._pool.Get()
	defer conn.Close()
	return redis.Float64(conn.Do("HINCRBYFLOAT", redis.Args{}.Add(s._key)...))
}
