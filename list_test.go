package redis_c

import (
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestList_PopBack(t *testing.T) {
	list := NewList("my_list_001", pool)
	push := "abc123"
	list.PushBack(push)
	pop, err := redis.String(list.PopBack())
	if err != nil {
		t.Error(err)
	}
	if pop == push {
		t.Log("PASS")
	} else {
		t.Error("Not Pass")
	}

}

func TestList_PopBackWait(t *testing.T) {
	list := NewList("my_list_001", pool)
	push := "abc123"
	list.PushBack(push)
	pop, err := redis.String(list.PopBackWait(10))
	if err != nil {
		t.Error(err)
	}
	if pop == push {
		t.Log("PASS")
	} else {
		t.Error("Not Pass")
	}
	pop, err = redis.String(list.PopBackWait(5))
	if err != nil {
		t.Error(err)
	}
	if pop == "" {
		t.Log("PASS")
	} else {
		t.Error("Not Pass")
	}

}
