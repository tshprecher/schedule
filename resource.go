package schedule

import (
	"sync"
)

// A Resource is something can be requested from and returned to a ResourcePool.
type Resource interface {
	// Return returns true iff the Resource was successfully
	// returned to the ResourcePool. A Resource can only be
	// returned once. Once returned, all subsequent calls
	// should return false.
	Return() bool
}

// A ResourcePool represents a pool of resources to be requested.
type ResourcePool interface {
	// Request takes a resource as a request and returns
	// a new resource if the request is granted, nil otherwise.
	// The returned resource can be returned with a call to Return()
	Request(r Resource) Resource
}

type resourceVector struct {
	pool      *resourceVectorPool
	resources []int
}

func (r *resourceVector) Return() bool {
	if r.pool == nil {
		return false
	}
	r.pool.add(r)
	r.pool = nil
	return true
}

func NewResourceVectorRequest(res []int) Resource {
	return &resourceVector{pool: nil, resources: res}
}

type resourceVectorPool struct {
	mut       *sync.Mutex
	resources []int
}

func NewResourceVectorPool(resources []int) *resourceVectorPool {
	return &resourceVectorPool{&sync.Mutex{}, resources}
}

func (r *resourceVectorPool) Request(res Resource) Resource {
	v, ok := res.(*resourceVector)
	if !ok || len(v.resources) != len(r.resources) {
		return nil
	}
	r.mut.Lock()
	defer r.mut.Unlock()
	for i := range r.resources {
		if v.resources[i] > r.resources[i] {
			return nil
		}
	}
	for i := range r.resources {
		r.resources[i] -= v.resources[i]
	}
	resources := make([]int, len(v.resources))
	copy(resources, v.resources)
	return &resourceVector{r, resources}
}

func (r *resourceVectorPool) add(v *resourceVector) bool {
	if len(r.resources) != len(v.resources) {
		return false
	}
	r.mut.Lock()
	defer r.mut.Unlock()
	for i := range r.resources {
		r.resources[i] += v.resources[i]
	}
	return true
}
