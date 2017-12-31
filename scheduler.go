package schedule

// Task represents an object to be queued.
type Task interface {
	Id() string
}

// ScheduledTask represents a Task leaving a Scheduler and is
// considered scheduled. Close() must be called upon completion
// to avoid leaking Resources.
type ScheduledTask interface {
	Id() string
	Task() Task
	Close()
}

// defaultScheduledTask implements a no-op Close()
type defaultScheduledTask struct {
	t Task
}

func (d *defaultScheduledTask) Task() Task { return d.t }

func (d *defaultScheduledTask) Id() string { return d.t.Id() }

func (d *defaultScheduledTask) Close() { return }

// A Scheduler manages a pool of tasks by returning them in a specified order
type Scheduler interface {
	// Contains returns true if and only if the scheduler contains the task
	Contains(t Task) bool

	// Put inserts each task in to the scheduler. If a task already exists with the id
	// the task is not replaced and the put is ignored.
	Put(t ...Task)

	// Next returns the next task wrapped in a ScheduledTask
	Next() ScheduledTask

	// Size returns the number of tasks present in the scheduler.
	Size() int

	// Remove removes the task with the given id. It returns nil if the scheduler
	// does not contain a task with that id.
	Remove(id string) Task
}

// A FifoScheduler is a scheduler that returns tasks in first in, first out (FIFO) order.
type FifoScheduler struct {
	elements            []Task
	elementMap          map[string]struct{}
	maxUnusedSliceSpace uint8
	unusedSliceCount    uint8
}

func NewFifoScheduler() *FifoScheduler {
	return &FifoScheduler{
		elements:            []Task{},
		elementMap:          map[string]struct{}{},
		maxUnusedSliceSpace: 16,
		unusedSliceCount:    0,
	}
}

func (f *FifoScheduler) Contains(t Task) bool {
	_, ok := f.elementMap[t.Id()]
	return ok
}

func (f *FifoScheduler) Put(tasks ...Task) {
	for _, t := range tasks {
		_, ok := f.elementMap[t.Id()]
		if !ok {
			f.elements = append(f.elements, t)
			f.unusedSliceCount++
			f.elementMap[t.Id()] = struct{}{}
		}
	}
	if f.unusedSliceCount >= f.maxUnusedSliceSpace {
		// reallocate the element slice so there's no memory leak
		newElements := make([]Task, len(f.elements))
		copy(newElements, f.elements)
		f.elements = newElements // reassign so old slice is garbage collected
		f.unusedSliceCount = 0
	}
}

func (f *FifoScheduler) Next() ScheduledTask {
	if len(f.elements) == 0 {
		return nil
	}
	s := f.elements[0]
	f.elements = f.elements[1:]
	delete(f.elementMap, s.Id())
	return &defaultScheduledTask{s}
}

func (f *FifoScheduler) Remove(id string) (t Task) {
	for e := range f.elements {
		if f.elements[e].Id() == id {
			t = f.elements[e]
			delete(f.elementMap, t.Id())
			f.elements = append(f.elements[:e], f.elements[e+1:]...)
			return
		}
	}
	return nil
}

func (f *FifoScheduler) Size() int {
	return len(f.elements)
}

type SchedulerFactory func() Scheduler

// A Partitioner is a function that takes a task and returns the partition of
// the task (key, priority) and a scheduler factory. This is used by PartitionedScheduler
// to route tasks to their proper schedulers.
type Partitioner func(t Task) (key string, priority uint, factory SchedulerFactory)

type partition struct {
	key   string
	value Scheduler
	cache map[string]struct{}
}
type priorityIterator struct {
	priority   uint
	partitions []partition
	pos        int
}

// A PartitionedScheduler partitions tasks into an arbitrary number of Schedulers
// as defined by the Partitioner and round robins over each partition, starting
// at the highest priorities first.
type PartitionedScheduler struct {
	partitioner           Partitioner
	prioritizedPartitions []*priorityIterator
}

func NewPartitionedScheduler(p Partitioner) *PartitionedScheduler {
	return &PartitionedScheduler{p, []*priorityIterator{}}
}

func (p *PartitionedScheduler) Contains(t Task) bool {
	for _, pi := range p.prioritizedPartitions {
		for _, part := range pi.partitions {
			if _, ok := part.cache[t.Id()]; ok {
				return true
			}
		}
	}
	return false
}

func (p *PartitionedScheduler) Put(tasks ...Task) {
	for _, t := range tasks {
		if p.Contains(t) {
			continue
		}
		key, pri, fact := p.partitioner(t)
		var iter *priorityIterator
		for i, pi := range p.prioritizedPartitions {
			if pi.priority == pri {
				iter = pi
				break
			} else if pi.priority < pri {
				newIter := &priorityIterator{pri, []partition{}, 0}
				p.prioritizedPartitions = append(p.prioritizedPartitions[:i], append([]*priorityIterator{newIter}, p.prioritizedPartitions[i:]...)...)
				iter = newIter
				break
			}
		}
		if iter == nil {
			newIter := &priorityIterator{pri, []partition{}, 0}
			p.prioritizedPartitions = append(p.prioritizedPartitions, newIter)
			iter = newIter
		}

		idx := -1
		for i := 0; i < len(iter.partitions); i++ {
			iter.pos = (iter.pos + 1) % len(iter.partitions)
			if iter.partitions[iter.pos].key == key {
				idx = iter.pos
				break
			}
		}
		if idx == -1 {
			iter.partitions = append(iter.partitions, partition{key, fact(), map[string]struct{}{}})
			iter.pos = len(iter.partitions) - 1
		}
		iter.partitions[iter.pos].cache[t.Id()] = struct{}{}
		iter.partitions[iter.pos].value.Put(t)
	}
}

func (p *PartitionedScheduler) Next() (t ScheduledTask) {
	for _, pi := range p.prioritizedPartitions {
		for i := 0; i < len(pi.partitions); i++ {
			idx := (pi.pos + i) % len(pi.partitions)
			t = pi.partitions[idx].value.Next()
			if t != nil {
				delete(pi.partitions[idx].cache, t.Task().Id())
				pi.pos = (pi.pos + i + 1) % len(pi.partitions)
				return
			}
		}
	}
	return
}

func (p *PartitionedScheduler) Remove(id string) (t Task) {
	for _, pri := range p.prioritizedPartitions {
		for _, prt := range pri.partitions {
			t = prt.value.Remove(id)
			if t != nil {
				delete(prt.cache, id)
				return
			}
		}
	}
	return
}

func (p *PartitionedScheduler) Size() (size int) {
	for _, pri := range p.prioritizedPartitions {
		for _, prt := range pri.partitions {
			size += prt.value.Size()
		}
	}
	return
}

// resourceTask is a ScheduledTask that attaches a task to the resource that
// has been granted to it. Upon completion, Close() returns the resource
// back to the pool.
type resourceTask struct {
	// TODO(tshprecher): make this wrap a ScheduledTask for proper chaining of Close()
	t        Task
	resource Resource
}

func (r *resourceTask) Task() Task { return r.t }

func (r *resourceTask) Id() string { return r.t.Id() }

// Close returns the resource associated with this ScheduledTask
func (r *resourceTask) Close() {
	r.resource.Return()
}

// A ResourceCalculator takes a task and returns the resource necessary
// to run it. The resource is not attached to a resource pool, but
// can be used to grant one via a call to ResourcePool.Request().
type ResourceCalculator func(Task) Resource

// A ResourceManagedScheduler returns the next task iff a resource exists
// to run it. If the necessary resource exists in the resource pool, the resource
// is requested from the pool and cleared when task.Close() is called.
type ResourceManagedScheduler struct {
	waiting            Task
	underlying         Scheduler
	pool               ResourcePool
	resourceCalculator ResourceCalculator
}

func NewResourceManagedScheduler(underlying Scheduler, pool ResourcePool, calc ResourceCalculator) *ResourceManagedScheduler {
	return &ResourceManagedScheduler{nil, underlying, pool, calc}
}

func (r *ResourceManagedScheduler) Contains(t Task) bool {
	return (r.waiting != nil && r.waiting.Id() == t.Id()) || r.underlying.Contains(t)
}

func (r *ResourceManagedScheduler) Put(tasks ...Task) {
	r.underlying.Put(tasks...)
}

func (r *ResourceManagedScheduler) Next() ScheduledTask {
	if r.waiting != nil {
		needed := r.resourceCalculator(r.waiting)
		allocated := r.pool.Request(needed)
		if allocated == nil {
			return nil
		}
		task := &resourceTask{r.waiting, allocated}
		r.waiting = nil
		return task
	}
	next := r.underlying.Next()
	if next == nil {
		return nil
	}
	needed := r.resourceCalculator(next.Task())
	allocated := r.pool.Request(needed)
	if allocated == nil {
		r.waiting = next.Task()
		return nil
	}
	return &resourceTask{next.Task(), allocated}
}

func (r *ResourceManagedScheduler) Remove(id string) Task {
	if r.waiting != nil && r.waiting.Id() == id {
		return r.waiting
	}
	return r.underlying.Remove(id)
}

func (r *ResourceManagedScheduler) Size() int {
	if r.waiting == nil {
		return r.underlying.Size()
	}
	return 1 + r.underlying.Size()
}
