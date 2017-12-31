# schedule

Systems with asynchronous tasks often need to schedule them to run in a specified order, whether FIFO or some more
complicated policy. When scheduling tasks, policies must consider fairness, latency, and resource management. While
policies can get increasingly complex, they should be easy to implement, test, and simulate. This library solves this
problem.

The Scheduler is the basic interface. Tasks are inserted in the order they are created and come out in the desired order.
With the proper composition of schedulers, complex policies are expressed with minimal extra code. This package includes
, but is not limited to, three basic schedulers: FifoScheduler, PartitionedScheduler, and ResourceManagedScheduler.

### FifoScheduler

The FifoScheduler is the simplest scheduler and behaves as a standard queue. It returns tasks in the order they were
inserted. It is often the basic building block in more complex scheduling policies.

### PartitionedScheduler

The PartitionedScheduler implements round-robinning and prioritization of multiple schedulers. Upon insertion, each task
is routed to a scheduler based on a mapping of each task to a partition key, priority level, and scheduler. A Partitioner
function defines the mapping. All tasks in priority k+1 are returned before priority k while round-robinning over each
scheduler within a priority level.

This can be used to implement fair policy over all users by setting the key to the user requesting each task.
See the examples below for more details.

### ResourceManagedScheduler

Real systems have resource limitations. If there are 1K pending tasks to complete, it may not be possible to run all of
them at the same time, even if the ordering of the tasks is fair. Past a certain point, the system can be overloaded or
deny the request to execute the task. There should be a way of managing resources over pending tasks. By estimating the
resource consumption of each task and available resources in the system, more efficient policies can be implemented to
increase throughput and latency.

This where ResourceManagedSchedulers come in. They serve to throttle tasks based on the predicted resources they consume
and the resources currently available. Each task returned from ResourceManagedSchedulers is bundled with a resource vector.
Upon calling ScheduledTask.Close(), the resources are returned to the resource pool managed by the scheduler. If no resources
are available to run the task, the scheduler returns nil until resources are freed.

In the examples below, this is used to separate faster tasks from slower tasks so fast tasks do not wait for slower
ones.

## Examples

With the basic Schedulers defined, let's consider the use case of many users querying a single database. The examples
here are included in github.com/tshprecher/schedule/sim_ex.go and can be simulated by running `sim_ed`. Let's assume
connections are the only resource to optimize over. Secondly, we want want fair round-robinning over user queries
so that one user's queries doesn't wait for all the other user's queries to complete before executing. To do this,
we can use ParitionedScheduler with the following partitioning function:

```go
func userPartitioner(t schedule.Task) (key string, priority uint, factory schedule.SchedulerFactory) {
	st := t.(*schedule.SimTask)
	key = strconv.Itoa(st.UserId)
	priority = 0
	factory = func() schedule.Scheduler {
		return schedule.NewFifoScheduler()
	}
	return
}
```

To simplify the example, the only resource we're considering are connections. Each query takes one connection, so mapping
a task to its resource utilization is easy:

```go
func singleUseResourceCalc(_ schedule.Task) schedule.Resource {
	return schedule.NewResourceVectorRequest([]int{1})
}
```

Finally, we instantiate the scheduler with a resource pool of two connections

```go
schedule.NewResourceManagedScheduler(schedule.NewPartitionedScheduler(userPartitioner), schedule.NewResourceVectorPool([]int{2}), singleUseResourceCalc)
```

Simulating a set of tasks through this scheduler, we get the following results:

```
*** Example 5
        Input:
                num users: 2
                resources: 2 tasks at a time
                policy: round-robin over user
                user 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}
                user 2 tasks: 10 with latencies {10ms, 20ms, 30ms, ..., 100ms}

        Results:
                user 1:
                        clock time:                      505 ms
                        throughput (tasks / sec):        19.801979
                user 2:
                        clock time:                      505 ms
                        throughput (tasks / sec):        19.801979
```

Notice it takes the same clock time to complete user 1's 10 queries as user 2's queries, but user 1's are each 10x
faster than user 2's. How can we minimize the effect of one user's slow queries affecting another user's fast queries?
An easy way would be set a threshold of fast queries, assign 1 connection each to fast and slow lanes, and partition over
the fast and slow queries before users. With a simple change of our partitioner, this is easily implemented. We will
consider 50ms as the slow cutoff.

```go
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
```

Therefore, the new scheduler is defined like this:

```go
schedule.NewPartitionedScheduler(timeAndUserPartitioner)
```

The resource partitioning is done in our new partitioner. Finally, simulation yields the following results:

```
*** Example 6
        Input:
                num users: 2
                resources: 2 tasks at a time
                policy: round-robin over user, 1 lane exclusively reserved for tasks taking at least 50ms
                user 1 tasks: 10 with latencies {1ms, 2ms, 3ms, ..., 10ms}
                user 2 tasks: 10 with latencies {10ms, 20ms, 30ms, ..., 100ms}

        Results:
                user 1:
                        clock time:                      327 ms
                        throughput (tasks / sec):        30.581039
                user 2:
                        clock time:                      597 ms
                        throughput (tasks / sec):        16.750420
```

With this simple change, user 1's throughput increases 50% while user 2's throughput decreases by just 15%. With further
data on users and query behavior, much more efficient policies can be developed and easily implemented.

## TODO

* ResourceManagedSchedulers currently only peek into one element from its underlying scheduler. This works fine
  where each task takes the same resources, but can block tasks with little resource utilization from running
  ahead of expensive tasks.