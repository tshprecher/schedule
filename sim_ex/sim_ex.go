package main

import (
	"fmt"
	"github.com/tshprecher/schedule"
	"strconv"
)

// userPartitioner partitions over user ids into FIFO schedulers, all with the same priority level.
func userPartitioner(t schedule.Task) (key string, priority uint, factory schedule.SchedulerFactory) {
	st := t.(*schedule.SimTask)
	key = strconv.Itoa(st.UserId)
	priority = 0
	factory = func() schedule.Scheduler {
		return schedule.NewFifoScheduler()
	}
	return
}

// timeAndUserPartitioner partitions tasks into fast and slow lanes, with each lane partitioned with userParitioner.
func timeAndUserPartitioner(t schedule.Task) (key string, priority uint, factory schedule.SchedulerFactory) {
	st := t.(*schedule.SimTask)
	key = "fast"
	if st.RuntimeMs >= 50 {
		key = "slow"
	}
	priority = 0
	factory = func() schedule.Scheduler {
		return schedule.NewResourceManagedScheduler(schedule.NewPartitionedScheduler(userPartitioner), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc)
	}
	return
}

func singleUseResourceCalc(_ schedule.Task) schedule.Resource {
	return schedule.NewResourceVectorRequest([]int{1})
}

func main() {
	fmt.Println("*** Example 1")
	fmt.Println("\tInput:")
	fmt.Println("\t\tnum users: 1")
	fmt.Println("\t\tresources: infinite")
	fmt.Println("\t\tpolicy: FIFO")
	fmt.Println("\t\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\n\tResults:")
	tasks := []*schedule.SimTask{}
	for i := 1; i <= 10; i++ {
		tasks = append(tasks, &schedule.SimTask{Identifier: i, UserId: 1, RuntimeMs: i})
	}
	schedule.Simulate(schedule.NewFifoScheduler(), tasks)
	fmt.Println()

	fmt.Println("*** Example 2")
	fmt.Println("\tInput:")
	fmt.Println("\t\tnum users: 1")
	fmt.Println("\t\tresources: 1 task at a time")
	fmt.Println("\t\tpolicy: FIFO")
	fmt.Println("\t\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\n\tResults:")
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewFifoScheduler(), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)
	fmt.Println()

	fmt.Println("*** Example 3")
	fmt.Println("\tInput:")
	fmt.Println("\t\tnum users: 2")
	fmt.Println("\t\tresources: 1 task at a time")
	fmt.Println("\t\tpolicy: FIFO")
	fmt.Println("\t\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\t\tuser 2 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\n\tResults:")
	for i := 11; i <= 20; i++ {
		tasks = append(tasks, &schedule.SimTask{Identifier: i, UserId: 2, RuntimeMs: i - 10})
	}
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewFifoScheduler(), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)
	fmt.Println()

	fmt.Println("*** Example 4")
	fmt.Println("\tInput:")
	fmt.Println("\t\tnum users: 2")
	fmt.Println("\t\tresources: 1 task at a time")
	fmt.Println("\t\tpolicy: round-robin over user")
	fmt.Println("\t\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\t\tuser 2 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\n\tResults:")
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewPartitionedScheduler(userPartitioner), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)
	fmt.Println()

	fmt.Println("*** Example 5")
	fmt.Println("\tInput:")
	fmt.Println("\t\tnum users: 2")
	fmt.Println("\t\tresources: 2 tasks at a time")
	fmt.Println("\t\tpolicy: round-robin over user")
	fmt.Println("\t\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\t\tuser 2 tasks: 10 with latencies {10ms, 20ms, 30ms, ..., 100ms}")
	fmt.Println("\n\tResults:")
	tasks = nil
	for i := 1; i <= 20; i++ {
		runtime := i
		if i > 10 {
			runtime = (runtime - 10) * 10
		}
		tasks = append(tasks, &schedule.SimTask{Identifier: i, UserId: (i-1)/10 + 1, RuntimeMs: runtime})
	}
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewPartitionedScheduler(userPartitioner), schedule.NewResourceVectorPool([]int{2}), singleUseResourceCalc), tasks)
	fmt.Println()

	fmt.Println("*** Example 6")
	fmt.Println("\tInput:")
	fmt.Println("\t\tnum users: 2")
	fmt.Println("\t\tresources: 2 tasks at a time")
	fmt.Println("\t\tpolicy: round-robin over user, 1 lane exclusively reserved for tasks taking at least 50ms")
	fmt.Println("\t\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\t\tuser 2 tasks: 10 with latencies {10ms, 20ms, 30ms, ..., 100ms}")
	fmt.Println("\n\tResults:")
	schedule.Simulate(schedule.NewPartitionedScheduler(timeAndUserPartitioner), tasks)
}
