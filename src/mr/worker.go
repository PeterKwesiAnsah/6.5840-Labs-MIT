package mr

import (
	"fmt"
	"hash/fnv"
	"net/rpc"
	"sync"
	"time"
)

var activeMaxMapTasks int = 8
var failedConns int = 0

type taskState int

// 2. Use iota to auto-increment values
const (
	active taskState = iota
	idle
)

type workerTask struct {
	state   taskState
	payload Task
}

type workerTasks struct {
	mu sync.Mutex
	ch chan struct {
		client *rpc.Client
	}
	tasks  []workerTask
	idle   int
	active int
	//??
	len int
}

func (wT *workerTasks) incrActive() {
	wT.active++
}
func (wT *workerTasks) decrActive() {
	wT.active--
}
func (wT *workerTasks) incrIdle() {
	wT.idle++
}
func (wT *workerTasks) decrIdle() {
	wT.idle--
}
func (wT *workerTasks) addTask(t workerTask) {
	wT.tasks = append(wT.tasks, t)
}
func (wT *workerTasks) setTaskStatus(t TaskId, status taskState) {
	wT.tasks[t].state = status
}
func (wT *workerTasks) setTaskPayload(t TaskId, payload Task) {
	wT.tasks[t].payload = payload
}

const (
	StatusSuccess = "success"
	StatusError   = "error"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
// A reasonable naming convention for intermediate files is mr-X-Y, where X is the Map task number, and Y is the reduce task number.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	var tasks workerTasks = workerTasks{ch: make(chan struct {
		client *rpc.Client
	}),
	}

	var done bool = false
	// This go routine will check the status of the job
	go func(status *bool) {
		connRetries := 0
		rpcRetries := 0
		for {
			if *status {
				break
			}
			if connRetries == 5 {
				*status = true
			}
			c, err := connectrpc()
			if err != nil {
				fmt.Printf(fmt.Errorf("Client Connection Failed for Job Status Check: %v\n", err).Error())
				connRetries++
				// How many retries before we give up
				continue
			}
			err = callrpc(c, "Coordinator.JobStatus", 0, status)
			if err != nil {
				fmt.Printf(fmt.Errorf("RPC Failed: %v\n", err).Error())
				// How many retries before we give up
				rpcRetries++
				c.Close()
				continue
			}
		}
	}(&done)

	// Each finished map task need to tell the co-ordinator
	// The only one that knows if the job is done is the co-ordinator
	connRetriesMapTasks := 0
	for {

		if done {
			break
		}
		if connRetriesMapTasks == 5 {
			done = true
		}
		c, err := connectrpc()
		if err != nil {
			fmt.Printf(fmt.Errorf("Client Connection Failed: %v\n", err).Error())
			connRetriesMapTasks++
			// How many retries before we give up
			continue
		}
		//use an idle thread or spawn a new one
	FIND_A_THREAD:
		tasks.mu.Lock()
		lenIdle := tasks.idle
		lenActive := tasks.active
		tasks.mu.Unlock()

		if lenActive == activeMaxMapTasks {
			time.Sleep(time.Second)
			goto FIND_A_THREAD
		} else if lenIdle != 0 {
			// wake up exactly one sleeping worker...we can't know or tell which woke up
			go func(client *rpc.Client) {
				tasks.ch <- struct {
					client *rpc.Client
				}{
					client,
				}
			}(c)
			continue
		}

		tasks.mu.Lock()
		tasks.addTask(workerTask{state: active})
		tasks.incrActive()
		tasks.mu.Unlock()

		var taskId TaskId = TaskId(lenActive)
		fmt.Printf("Spawning Client Worker Task %d\n", taskId)
		go func(client *rpc.Client, taskId TaskId, status *bool) {
			defer func() {
				// we can check for crashes and handle appropiately
				// died go routines are neither idle / active
				tasks.mu.Lock()
				tasks.decrIdle()
				tasks.decrActive()
				tasks.mu.Unlock()
			}()
			for {
				var path Task
				err := callrpc(client, "Coordinator.GetTask", int(taskId), &path)
				if err != nil {
					fmt.Printf(fmt.Errorf("Task Id %d: %v\n", taskId, err).Error())
					if err.Error() == "EEMPTY" {
						*status = true
					}
					return
				}
				//do work with Task
				c.Close()
				fmt.Printf("Client Worker Task %d just finished with %s\n", taskId, path.Path)
				// report when finished, have a service name to report task finish
				// active -> idle
				tasks.mu.Lock()
				tasks.setTaskPayload(taskId, path)
				tasks.setTaskStatus(taskId, idle)
				tasks.incrIdle()
				tasks.decrActive()
				tasks.mu.Unlock()

				fmt.Printf("Sleeping Client Worker Task %d\n", taskId)
				v := <-tasks.ch
				// woke up
				client = v.client
				fmt.Printf("Waking up Client Worker Task %d\n", taskId)
				// idle -> active
				tasks.mu.Lock()
				tasks.setTaskStatus(taskId, active)
				tasks.decrIdle()
				tasks.incrActive()
				tasks.mu.Unlock()
			}
		}(c, taskId, &done)
	}
	// reducers
	println("Job Done")

}

// send an RPC request to the coordinator, wait for the response.
func callrpc(c *rpc.Client, rpcname string, arg any, reply any) error {
	err := c.Call(rpcname, arg, reply)
	//blocking
	if err != nil {
		return err
	}

	return nil
}

func connectrpc() (*rpc.Client, error) {
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		return nil, err
	}
	return c, nil
}
