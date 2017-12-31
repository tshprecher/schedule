package schedule

import (
	"testing"
)

func TestResourceVectorPoolRequest(t *testing.T) {
	pool := NewResourceVectorPool([]int{1, 2})
	requesting := &resourceVector{resources: []int{0, 0}}
	returned := pool.Request(requesting)
	if returned == nil {
		t.Error("expected valid resource request")
	}
	if !(pool.resources[0] == 1 && pool.resources[1] == 2) {
		t.Error("unexpected pool resource values")
	}

	requesting = &resourceVector{resources: []int{2, 0}}
	returned = pool.Request(requesting)
	if returned != nil {
		t.Error("expected invalid resource request")
	}
	if !(pool.resources[0] == 1 && pool.resources[1] == 2) {
		t.Error("unexpected pool resource values")
	}

	requesting = &resourceVector{resources: []int{1, 0}}
	returned = pool.Request(requesting)
	if returned == nil {
		t.Error("expected valid resource request")
	}
	if !(pool.resources[0] == 0 && pool.resources[1] == 2) {
		t.Error("unexpected pool resource values")
	}

	requesting = &resourceVector{resources: []int{1}}
	returned = pool.Request(requesting)
	if returned != nil {
		t.Error("expected invalid resource request")
	}
	if !(pool.resources[0] == 0 && pool.resources[1] == 2) {
		t.Error("unexpected pool resource values")
	}
}

func TestResourceVectorReturn(t *testing.T) {
	pool := NewResourceVectorPool([]int{1, 2})
	requesting := &resourceVector{resources: []int{1, 0}}
	returned := pool.Request(requesting)
	if !(pool.resources[0] == 0 && pool.resources[1] == 2) {
		t.Error("unexpected pool resource values")
	}
	vec := returned.(*resourceVector)
	if vec.pool == nil {
		t.Error("expected pool present")
	}
	if !(vec.resources[0] == 1 && vec.resources[1] == 0) {
		t.Error("unexpected vector resources")
	}

	// return the first time should replenish the pool of the resources
	res := vec.Return()
	if !res {
		t.Error("expected successful return")
	}
	if vec.pool != nil {
		t.Error("expected pool not present")
	}
	if !(pool.resources[0] == 1 && pool.resources[1] == 2) {
		t.Error("unexpected pool resource values")
	}

	// return a second time should be idempotent
	res = vec.Return()
	if res {
		t.Error("expected unsuccessful return")
	}
	if !(pool.resources[0] == 1 && pool.resources[1] == 2) {
		t.Error("unexpected pool resource values")
	}
}
