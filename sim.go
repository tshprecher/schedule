package schedule

import (
	"strconv"
	"fmt"
)

type SimTask struct {
	Identifier int
	UserId int
	RuntimeMs int
}

func (s *SimTask) Id() string {
	return strconv.Itoa(s.Identifier)
}

// Simulate takes a scheduler and a slice of SimTasks, simulates
// the runtime of those tasks as they are removed from the scheduler,
// and prints latency results to standard output.
func Simulate(scheduler Scheduler, tasks []*SimTask) {
	for _, t := range tasks {
		scheduler.Put(t)
	}
	currentTimeMs := 0
	endtimesPerUser := make(map[int][]int)
	taskLatencyPerUser := make(map[int][]int)
	runningTasks := map[ScheduledTask]int{}
	for scheduler.Size() > 0 || len(runningTasks) > 0 {
		if (scheduler.Size() > 0) {
			for nextTask := scheduler.Next(); nextTask != nil; nextTask = scheduler.Next() {
				st := nextTask.Task().(*SimTask)
				runningTasks[nextTask] = currentTimeMs + st.RuntimeMs
			}
		}
		if len(runningTasks) > 0 {
			// simulate completion of shortest task
			earliestCompTimeMs := -1
			earliestCompTimeTasks := []ScheduledTask{}
			for ta, tm := range runningTasks {
				if (earliestCompTimeMs == -1 || tm < earliestCompTimeMs) {
					earliestCompTimeMs = tm
					earliestCompTimeTasks = nil
				}
				if tm == earliestCompTimeMs {
					earliestCompTimeTasks = append(earliestCompTimeTasks, ta)
				}
			}
			if len(earliestCompTimeTasks) > 0 {
				currentTimeMs += earliestCompTimeTasks[0].Task().(*SimTask).RuntimeMs
				for i := range (earliestCompTimeTasks) {
					st := earliestCompTimeTasks[i].Task().(*SimTask)
					endtimesPerUser[st.UserId] = append(endtimesPerUser[st.UserId], earliestCompTimeMs)
					taskLatencyPerUser[st.UserId] = append(taskLatencyPerUser[st.UserId], currentTimeMs)
					earliestCompTimeTasks[i].Close()
					delete(runningTasks, earliestCompTimeTasks[i])
				}
			}
		}
	}
	userIds := []int{}
	for k := range(endtimesPerUser) {
		userIds = append(userIds, k)
		for i := len(userIds)-1; i > 0 && userIds[i] < userIds[i-1]; i-- {
			temp := userIds[i]
			userIds[i] = userIds[i-1]
			userIds[i-1] = temp
		}
	}

	for _, id := range(userIds) {
		et := endtimesPerUser[id]
		fmt.Printf("user %d:\n", id)
		fmt.Printf("\tclock time:\t\t\t %d ms\n", et[len(et)-1])
		fmt.Printf("\tthroughput (tasks / sec):\t %f\n", float32(len(et)) / float32(et[len(et)-1]) * 1000)
	}
}


