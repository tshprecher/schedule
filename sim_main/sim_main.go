package main

import (
	"fmt"
	"temp/schedule"
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

func singleUseResourceCalc(_ schedule.Task) schedule.Resource {
	return schedule.NewResourceVectorRequest([]int{1})
}

func main() {
	fmt.Print("*** Total latency with 1 user, no resource constraints, 10 tasks with latencies {1ms, 2ms, 3ms, ..., 10ms}:\n")
	tasks := []*schedule.SimTask{}
	for i := 1; i <= 10; i++ {
		tasks = append(tasks, &schedule.SimTask{Identifier: i, UserId: 1, RuntimeMs: i})
	}
	schedule.Simulate(schedule.NewFifoScheduler(), tasks)

	fmt.Println()
	fmt.Print("*** Total latency with 1 user, 1 task at a time, 10 tasks with latencies {1ms, 2ms, 3ms, ..., 10ms}:\n")
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewFifoScheduler(), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)

	fmt.Println()
	fmt.Print("*** Total latency with 2 users, 1 task at a time, 10 tasks/user with latencies {1ms, 2ms, 3ms, ..., 10ms}:\n")
	for i := 11; i <= 20; i++ {
		tasks = append(tasks, &schedule.SimTask{Identifier: i, UserId: 2, RuntimeMs: i-10})
	}
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewFifoScheduler(), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)

	fmt.Println()
	fmt.Print("*** Total latency with 2 users, 1 task at a time, round robin over user, 10 tasks/user with latencies {1ms, 2ms, 3ms, ..., 10ms}:\n")
	schedule.Simulate(schedule.NewResourceManagedScheduler(schedule.NewPartitionedScheduler(userPartitioner), schedule.NewResourceVectorPool([]int{1}), singleUseResourceCalc), tasks)
}
