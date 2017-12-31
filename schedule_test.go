package schedule

import (
	"fmt"
	"testing"
)

type testTask struct {
	field int
}

func (t testTask) Id() string {
	return fmt.Sprintf("%d", t.field)
}

func (t testTask) Done() {}

func expectSizeEquals(t *testing.T, scheduler Scheduler, size int) {
	if scheduler.Size() != size {
		t.Errorf("expected size %d, received %d", size, scheduler.Size())
	}
}

func expectContains(t *testing.T, scheduler Scheduler, task Task, contains bool) {
	if scheduler.Contains(task) != contains {
		t.Errorf("expected contains %v, received %v", contains, scheduler.Contains(task))
	}
}

func expectNilTask(t *testing.T, task Task) {
	if task != nil {
		t.Errorf("expected nil task, received %v", task)
	}
}

func expectNotNilTask(t *testing.T, task Task) {
	if task == nil {
		t.Errorf("expected not nil task, received %v", task)
	}
}

func expectTaskEquals(t *testing.T, taskOne, taskTwo Task) {
	t1 := taskOne.(testTask)
	t2 := taskTwo.(testTask)
	if t1 != t2 {
		t.Error("expected equal tasks")
	}
}

func testCommonDupTask(t *testing.T, scheduler Scheduler) {
	// does not add duplicate task
	scheduler.Put(testTask{1})
	scheduler.Put(testTask{1})
	expectSizeEquals(t, scheduler, 1)
	scheduler.Next()
	expectSizeEquals(t, scheduler, 0)
	scheduler.Put(testTask{1})
	expectSizeEquals(t, scheduler, 1)
}

func testCommonSize(t *testing.T, scheduler Scheduler) {
	expectSizeEquals(t, scheduler, 0)
	scheduler.Put(testTask{1}, testTask{2})
	expectSizeEquals(t, scheduler, 2)
	scheduler.Next()
	expectSizeEquals(t, scheduler, 1)
	scheduler.Next()
	expectSizeEquals(t, scheduler, 0)
}

func testCommonContains(t *testing.T, scheduler Scheduler) {
	scheduler.Put(testTask{1})
	expectContains(t, scheduler, testTask{field: 1}, true)
	expectContains(t, scheduler, testTask{field: 2}, false)
	scheduler.Put(testTask{2})
	scheduler.Put(testTask{3})
	expectContains(t, scheduler, testTask{field: 2}, true)
	expectContains(t, scheduler, testTask{field: 3}, true)

	scheduler.Remove(testTask{1}.Id())
	expectContains(t, scheduler, testTask{field: 1}, false)
	expectNotNilTask(t, scheduler.Next())
	expectNotNilTask(t, scheduler.Next())

	expectContains(t, scheduler, testTask{field: 2}, false)
	expectContains(t, scheduler, testTask{field: 3}, false)
}

func testCommonRemove(t *testing.T, scheduler Scheduler) {
	scheduler.Put(testTask{1})
	scheduler.Put(testTask{2})
	scheduler.Put(testTask{3})
	expectSizeEquals(t, scheduler, 3)

	expectNilTask(t, scheduler.Remove(testTask{4}.Id()))
	expectTaskEquals(t, scheduler.Remove(testTask{2}.Id()), testTask{field: 2})
	expectSizeEquals(t, scheduler, 2)
	expectNotNilTask(t, scheduler.Next())
	expectNotNilTask(t, scheduler.Next())
	expectNilTask(t, scheduler.Next())
	expectSizeEquals(t, scheduler, 0)
}

func TestFifoScheduler(t *testing.T) {
	// common
	testCommonDupTask(t, NewFifoScheduler())
	testCommonSize(t, NewFifoScheduler())
	testCommonContains(t, NewFifoScheduler())
	testCommonRemove(t, NewFifoScheduler())

	// returns items in the order they were inserted
	scheduler := NewFifoScheduler()
	scheduler.Put(testTask{1}, testTask{2})
	expectTaskEquals(t, scheduler.Next().Task(), testTask{1})
	expectTaskEquals(t, scheduler.Next().Task(), testTask{2})
	expectNilTask(t, scheduler.Next())
}

func TestPartitionedScheduler(t *testing.T) {
	schedulerFactory := func() Scheduler {
		return NewFifoScheduler()
	}
	noPriPartitioner := func(t Task) (string, uint, SchedulerFactory) {
		testTask := t.(testTask)
		if testTask.field%2 == 0 {
			return "even", 1, schedulerFactory
		}
		return "odd", 1, schedulerFactory
	}

	var priPartitioner Partitioner = func(t Task) (string, uint, SchedulerFactory) {
		testTask := t.(testTask)
		if testTask.field%3 == 0 {
			return "rem_0", 3, schedulerFactory
		} else if testTask.field%3 == 1 {
			return "rem_1", 2, schedulerFactory
		} else {
			return "rem_2", 1, schedulerFactory
		}
	}

	// test common no priority partitioner
	testCommonDupTask(t, NewPartitionedScheduler(noPriPartitioner))
	testCommonSize(t, NewPartitionedScheduler(noPriPartitioner))
	testCommonContains(t, NewPartitionedScheduler(noPriPartitioner))
	testCommonRemove(t, NewPartitionedScheduler(noPriPartitioner))

	// test common priority partitioner
	testCommonDupTask(t, NewPartitionedScheduler(priPartitioner))
	testCommonSize(t, NewPartitionedScheduler(priPartitioner))
	testCommonContains(t, NewPartitionedScheduler(priPartitioner))
	testCommonRemove(t, NewPartitionedScheduler(priPartitioner))

	// round robin over partitions
	noPriScheduler := NewPartitionedScheduler(noPriPartitioner)
	noPriScheduler.Put(testTask{1})
	noPriScheduler.Put(testTask{3})
	noPriScheduler.Put(testTask{2})
	noPriScheduler.Put(testTask{4})
	noPriScheduler.Put(testTask{5})

	expectTaskEquals(t, noPriScheduler.Next().Task(), testTask{1})
	expectTaskEquals(t, noPriScheduler.Next().Task(), testTask{2})
	expectTaskEquals(t, noPriScheduler.Next().Task(), testTask{3})
	expectTaskEquals(t, noPriScheduler.Next().Task(), testTask{4})
	expectTaskEquals(t, noPriScheduler.Next().Task(), testTask{5})

	// returns highest priority elements first
	priScheduler := NewPartitionedScheduler(priPartitioner)
	priScheduler.Put(testTask{1})
	priScheduler.Put(testTask{2})
	priScheduler.Put(testTask{3})
	priScheduler.Put(testTask{4})
	priScheduler.Put(testTask{5})
	priScheduler.Put(testTask{6})

	expectTaskEquals(t, priScheduler.Next().Task(), testTask{3})
	expectTaskEquals(t, priScheduler.Next().Task(), testTask{6})
	expectTaskEquals(t, priScheduler.Next().Task(), testTask{1})
	expectTaskEquals(t, priScheduler.Next().Task(), testTask{4})
	expectTaskEquals(t, priScheduler.Next().Task(), testTask{2})
	expectTaskEquals(t, priScheduler.Next().Task(), testTask{5})
}

func TestResourceManagedScheduler(t *testing.T) {
	var calc ResourceCalculator = func(t Task) Resource {
		return &resourceVector{resources: []int{1}}
	}
	testCommonDupTask(t, NewResourceManagedScheduler(NewFifoScheduler(), NewResourceVectorPool([]int{2}), calc))
	testCommonSize(t, NewResourceManagedScheduler(NewFifoScheduler(), NewResourceVectorPool([]int{2}), calc))
	testCommonContains(t, NewResourceManagedScheduler(NewFifoScheduler(), NewResourceVectorPool([]int{2}), calc))
	testCommonRemove(t, NewResourceManagedScheduler(NewFifoScheduler(), NewResourceVectorPool([]int{2}), calc))

	// Next() returns nil if no resources exist to schedule the task
	scheduler := NewResourceManagedScheduler(NewFifoScheduler(), NewResourceVectorPool([]int{2}), calc)
	scheduler.Put(testTask{1})
	scheduler.Put(testTask{2})
	scheduler.Put(testTask{3})
	nextOne := scheduler.Next()
	nextTwo := scheduler.Next()
	expectTaskEquals(t, nextOne.Task(), testTask{1})
	expectTaskEquals(t, nextTwo.Task(), testTask{2})
	nextThree := scheduler.Next()
	expectNilTask(t, nextThree)
	expectSizeEquals(t, scheduler, 1)
	nextOne.Close()
	expectTaskEquals(t, scheduler.Next().Task(), testTask{3})

	// checks if the waiting element has a task
	scheduler = NewResourceManagedScheduler(NewFifoScheduler(), NewResourceVectorPool([]int{2}), calc)
	expectContains(t, scheduler, testTask{1}, false)
	scheduler.waiting = testTask{1}
	expectContains(t, scheduler, testTask{1}, true)
	expectTaskEquals(t, scheduler.Next().Task(), testTask{1})
	expectContains(t, scheduler, testTask{1}, false)
}
