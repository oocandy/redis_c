package redis_c

import (
	"fmt"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	pool *redis.Pool
)

//初始化一个pool
func NewPool(server, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		MaxActive:   5,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func InitTest() {
	pool = NewPool("127.0.0.1:6379", "")
}

func TestMain(m *testing.M) {
	fmt.Println("begin")
	InitTest()
	m.Run()
	fmt.Println("end")
}

func TestHash_Del(t *testing.T) {
	hash := NewHash("my_hash1", pool)
	name := "n1"
	hash.Set(name, "eee")
	if ok, err := hash.Del(name); err == nil && ok {
		t.Log("DEL PASS")
	} else {
		t.Error("DEL NOT PASS")
	}
}

func TestHash_Dels(t *testing.T) {
	hash := NewHash("my_hash2", pool)
	pairs := map[interface{}]interface{}{
		"n1": "v1",
		"n2": "v2",
		"n3": "v3",
	}
	err := hash.Sets(pairs)
	if err != nil {
		t.Error("DEL NOT PASS")
	}
	if count, err := hash.Dels("n1", "n2", "n3"); err == nil && count == 3 {
		t.Log("Dels PASS")
	} else {
		t.Error("Dels NOT PASS")
	}
}

func TestHash_Get(t *testing.T) {
	hash := NewHash("my_hash2", pool)
	name := "n1"
	value := "abc12345567"
	err := hash.Set(name, value)
	if err != nil {
		t.Error("Get NOT PASS")
	}
	if value2, err := redis.String(hash.Get(name)); err == nil && value2 == value {
		t.Log("Get PASS")
	} else {
		t.Error("Get NOT PASS")
	}
}

func TestHash_Gets(t *testing.T) {
	hash := NewHash("my_hash2", pool)
	pairs := map[interface{}]interface{}{
		"n1": "v1",
		"n2": "v2",
		"n3": "v3",
	}
	err := hash.Sets(pairs)
	if err != nil {
		t.Error("Gets NOT PASS")
	}
	if vs, err := redis.Strings(hash.Gets("n1", "n2", "n3")); err == nil {
		if len(vs) == 3 &&
			vs[0] == "v1" &&
			vs[1] == "v2" &&
			vs[2] == "v3" {

			t.Log("Gets PASS")
		} else {
			t.Error("Gets NOT PASS")
		}
	} else {
		t.Error("Gets NOT PASS")
	}
}

func TestHash_Set(t *testing.T) {
	hash := NewHash("my_hash2", pool)
	name := "n1"
	value := "abc12345567"
	err := hash.Set(name, value)
	if err != nil {
		t.Error("Set NOT PASS")
	}
	if value2, err := redis.String(hash.Get(name)); err == nil && value2 == value {
		t.Log("Set PASS")
	} else {
		t.Error("Set NOT PASS")
	}
}

func TestHash_Sets(t *testing.T) {
	hash := NewHash("my_hash2", pool)
	pairs := map[interface{}]interface{}{
		"n1": "v1",
		"n2": "v2",
		"n3": "v3",
	}
	err := hash.Sets(pairs)
	if err != nil {
		t.Error("Sets NOT PASS")
	}
	if vs, err := redis.Strings(hash.Gets("n1", "n2", "n3")); err == nil {
		if len(vs) == 3 &&
			vs[0] == "v1" &&
			vs[1] == "v2" &&
			vs[2] == "v3" {

			t.Log("Sets PASS")
		} else {
			t.Error("Sets NOT PASS")
		}
	} else {
		t.Error("Sets NOT PASS")
	}
}

func TestHash_Size(t *testing.T) {
	hash := NewHash("my_hash11", pool)
	pairs := map[interface{}]interface{}{
		"n1": "v1",
		"n2": "v2",
		"n3": "v3",
	}
	err := hash.Sets(pairs)
	if err != nil {
		t.Error("Size NOT PASS")
	}

	if Count, err := hash.Count(); err == nil && Count == 3 {
		t.Log("Size PASS")
	} else {
		t.Error("Size NOT PASS")
	}
}

func TestHash_Clear(t *testing.T) {
	hash := NewHash("my_hash2", pool)
	pairs := map[interface{}]interface{}{
		"n1": "v1",
		"n2": "v2",
		"n3": "v3",
	}
	err := hash.Sets(pairs)
	if err != nil {
		if Count, err := hash.Count(); err == nil && Count == 0 {
			t.Log("Clear PASS")
		} else {
			t.Error("Clear NOT PASS")
		}
	}
	ok, err := hash.Clear()
	if err == nil && ok {
		t.Log("Clear PASS")
	}
}

func TestHash_Names(t *testing.T) {
	hash := NewHash("my_hash12", pool)
	pairs := map[interface{}]interface{}{
		"n1": "v1",
		"n2": "v2",
		"n3": "v3",
	}
	err := hash.Sets(pairs)
	if err != nil {
		t.Error("Names NOT PASS")
	}
	names, err := redis.Strings(hash.Names())
	if err == nil {
		if len(names) == 3 &&
			names[0] == "n1" &&
			names[1] == "n2" &&
			names[2] == "n3" {
			t.Log("Names PASS")
		} else {
			t.Error("Names NOT PASS")
		}
	} else {
		t.Error("Names NOT PASS")
	}
}

func TestHash_Values(t *testing.T) {
	hash := NewHash("my_hash12", pool)
	pairs := map[interface{}]interface{}{
		"n1": "v1",
		"n2": "v2",
		"n3": "v3",
	}
	err := hash.Sets(pairs)
	if err != nil {
		t.Error("Values NOT PASS")
	}
	values, err := redis.Strings(hash.Values())
	if err == nil {
		if len(values) == 3 &&
			values[0] == "v1" &&
			values[1] == "v2" &&
			values[2] == "v3" {
			t.Log("Values PASS")
		} else {
			t.Error("Values NOT PASS")
		}
	} else {
		t.Error("Values NOT PASS")
	}
}

func TestHash_Pairs(t *testing.T) {
	hash := NewHash("my_hash1999", pool)
	pairs := map[interface{}]interface{}{
		"n1": "v1",
		"n2": "v2",
		"n3": "v3",
	}
	err := hash.Sets(pairs)
	if err != nil {
		t.Error("Pairs NOT PASS")
	}
	strMap, err := redis.StringMap(hash.Pairs())
	if err == nil {
		L := len(strMap)
		if L == 3 {
			if strMap["n1"] == "v1" && strMap["n2"] == "v2" && strMap["n3"] == "v3" {
				t.Log("Pairs PASS")
			} else {
				t.Error("Pairs NOT PASS")
			}
		} else {
			t.Error("Pairs NOT PASS")
		}

	} else {
		t.Error("Pairs NOT PASS")
	}
}
