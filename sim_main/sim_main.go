package main

import (
	"fmt"
	"github.com/tshprecher/schedule"
	"strconv"
)

func userPartitioner(t schedule.Task) (key string, priority uint, factory schedule.SchedulerFactory) {
	st := t.(*schedule.SimTask)
	key = strconv.Itoa(st.UserId)
	priority = 0
	factory = func() schedule.Scheduler {
		return schedule.NewFifoScheduler()
	}
	return
}

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
	fmt.Println("Input:")
	fmt.Println("\tnum users: 1")
	fmt.Println("\tresources: infinite")
	fmt.Println("\tpolicy: FIFO")
	fmt.Println("\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\nResults:")
	tasks := []*schedule.SimTask{}
	for i := 1; i <= 10; i++ {
		tasks = append(tasks, &schedule.SimTask{Identifier: i, UserId: 1, RuntimeMs: i})
	}
	schedule.Simulate(schedule.NewFifoScheduler(), tasks)
	fmt.Println()

	fmt.Println("*** Example 2")
	fmt.Println("Input:")
	fmt.Println("\tnum users: 1")
	fmt.Println("\tresources: 1 task at a time")
	fmt.Println("\tpolicy: FIFO")
	fmt.Println("\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\nResults:")
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewFifoScheduler(), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)
	fmt.Println()

	fmt.Println("*** Example 3")
	fmt.Println("Input:")
	fmt.Println("\tnum users: 2")
	fmt.Println("\tresources: 1 task at a time")
	fmt.Println("\tpolicy: FIFO")
	fmt.Println("\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\tuser 2 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\nResults:")
	for i := 11; i <= 20; i++ {
		tasks = append(tasks, &schedule.SimTask{Identifier: i, UserId: 2, RuntimeMs: i-10})
	}
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewFifoScheduler(), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)
	fmt.Println()

	fmt.Println("*** Example 4")
	fmt.Println("Input:")
	fmt.Println("\tnum users: 2")
	fmt.Println("\tresources: 1 task at a time")
	fmt.Println("\tpolicy: round-robin over user")
	fmt.Println("\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\tuser 2 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\nResults:")
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewPartitionedScheduler(userPartitioner), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)
	fmt.Println()

	fmt.Println("*** Example 5")
	fmt.Println("Input:")
	fmt.Println("\tnum users: 2")
	fmt.Println("\tresources: 2 tasks at a time")
	fmt.Println("\tpolicy: round-robin over user")
	fmt.Println("\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\tuser 2 tasks: 10 with latencies {10ms, 20ms, 30ms, ..., 100ms}")
	fmt.Println("\nResults:")
	tasks = nil
	for i := 1; i <= 20; i++ {
		runtime := i
		if i > 10 {
			runtime = (runtime-10) * 10
		}
		tasks = append(tasks, &schedule.SimTask{Identifier: i, UserId: (i-1)/10+1, RuntimeMs: runtime})
	}
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewPartitionedScheduler(userPartitioner), schedule.NewResourceVectorPool([]int{2}), singleUseResourceCalc), tasks)
	fmt.Println()

	fmt.Println("*** Example 6")
	fmt.Println("Input:")
	fmt.Println("\tnum users: 2")
	fmt.Println("\tresources: 2 tasks at a time")
	fmt.Println("\tpolicy: round-robin over user, 1 lane exclusively reserved for tasks taking at least 50ms")
	fmt.Println("\tuser 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}")
	fmt.Println("\tuser 2 tasks: 10 with latencies {10ms, 20ms, 30ms, ..., 100ms}")
	fmt.Println("\nResults:")
	schedule.Simulate(schedule.NewPartitionedScheduler(timeAndUserPartitioner), tasks)
}
