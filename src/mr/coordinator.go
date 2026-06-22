package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"sync"
	"time"
)

type workerState int

const (
	in_progress workerState = iota
	completed
	failed
)

type taskStatus struct {
	//workerId WorkerId
	state workerState
	info  WorkerInfo
}

// TODO: Read/Write locks for mTasks and rTasks
type Coordinator struct {
	mTasks *[][]taskStatus
	//M
	mInputs []string
	//R
	mOutput *[][]string
	rTasks  *[][]taskStatus
	mu      sync.Mutex
}

var reduceTasksCnt int
var coordinatorTasks = 0

// It's use have a similar behavior like a sem
var availWorkers chan int

var completedMTasks chan int
var completedRTasks chan int

// in-progress
var workers chan struct {
	info   WorkerInfo
	taskId int
}

// idle/non-active/nil -> in_progress
func (c *Coordinator) GetrTask(arg WorkerInfo, reply *RTask) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// TODO: markTaskProgress
	for reduceTask := range reduceTasksCnt {
		rTask := (*c.rTasks)[reduceTask]
		if rTask == nil {
			*reply = RTask{
				MapOutputs: (*c.mOutput)[reduceTask],
				ReduceId:   reduceTask,
			}
			(*c.rTasks)[reduceTask] = []taskStatus{{info: arg, state: in_progress}}
			workers <- struct {
				info   WorkerInfo
				taskId int
			}{info: arg, taskId: reduceTask}
			return nil
		} else {
			lastStatus := rTask[len(rTask)-1]
			if lastStatus.state == failed {
				rTask = append(rTask, taskStatus{state: in_progress, info: arg})
				(*c.rTasks)[reduceTask] = rTask
				*reply = RTask{
					MapOutputs: (*c.mOutput)[reduceTask],
					ReduceId:   reduceTask,
				}
				workers <- struct {
					info   WorkerInfo
					taskId int
				}{info: arg, taskId: reduceTask}
				return nil
			}
		}
	}
	// when we hit here should we let the scheduler know ??
	//TODO; change this for backup Tasks (3.6)
	return fmt.Errorf("All rtasks are inprogress\n")
}

// idle/non-active/nil -> in_progress
func (c *Coordinator) GetmTask(arg WorkerInfo, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for index, inputFile := range c.mInputs {
		mTask := (*c.mTasks)[index]
		if mTask == nil {
			(*c.mTasks)[index] = []taskStatus{
				{info: arg, state: in_progress},
			}
			*reply = Task{
				Path:        inputFile,
				ReduceTasks: reduceTasksCnt,
				TaskId:      index,
			}
			workers <- struct {
				info   WorkerInfo
				taskId int
			}{info: arg, taskId: index}
			return nil
		} else {
			lastStatus := mTask[len(mTask)-1]
			if lastStatus.state == failed {
				mTask = append(mTask, taskStatus{info: arg, state: in_progress})
				(*c.mTasks)[index] = mTask
				*reply = Task{
					Path:        inputFile,
					ReduceTasks: reduceTasksCnt,
					TaskId:      index,
				}
				workers <- struct {
					info   WorkerInfo
					taskId int
				}{info: arg, taskId: index}
				return nil
			}
		}
	}

	// when we hit here should we let the scheduler know ?? Yes
	//TODO: change this for backup Tasks (3.6)
	return fmt.Errorf("All tasks are inprogress\n")
}

// in_progress -> completed
func (c *Coordinator) Notify(arg NotifyTask, reply *ReceivedNotofication) error {
	if reduceTasksCnt != len(arg.ReduceTasks) {
		*reply = -1 //INSUF_RT
		return fmt.Errorf("Expected %d but got %d\n", reduceTasksCnt, len(arg.ReduceTasks))
	}

	<-availWorkers
	c.mu.Lock()
	defer c.mu.Unlock()

	//TODO: refactor markTaskCompleted
	//completedMTasks++
	mTask := (*c.mTasks)[arg.MapTask.TaskId]
	mTask = append(mTask, taskStatus{info: WorkerInfo{WorkerId: int(arg.WorkerId), Type: 0}, state: completed})
	(*c.mTasks)[arg.MapTask.TaskId] = mTask
	*reply = 0
	// Produce a single reduce task per partition
	// TODO: we can schedule reduce workers from here. In the paper reduce and map workers run concurrently.
	for index, reduceTask := range arg.ReduceTasks {
		reduceTasks := (*c.mOutput)[index]
		if reduceTasks == nil {
			(*c.mOutput)[index] = make([]string, 0, reduceTasksCnt)
			reduceTasks = (*c.mOutput)[index]
		}
		reduceTasks = append(reduceTasks, reduceTask)
		(*c.mOutput)[index] = reduceTasks
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if coordinatorTasks == reduceTasksCnt {
		return true
	}
	<-completedRTasks
	coordinatorTasks++
	return false
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v\n", sockname, e)
	}
	//fmt.Printf("listening at %s\n", sockname)
	go http.Serve(l, nil)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// TODO: 3.6 (Backup Tasks)
// TODO: 4.2 (Ordering Guarantees)
// TODO: 4.3 (Combiner Function)
// TODO: 4.8 (Status Information)
// TODO: 4.9 (Counters)
// TODO: implement task locking, remove global lock
// TODO: implement tasks methods
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	defer func() {
		//remove produced files
	}()

	M := len(files)
	completedMTasks = make(chan int, M)
	completedRTasks = make(chan int, nReduce)
	reduceTasksCnt = nReduce
	availWorkers = make(chan int, 4)
	workers = make(chan struct {
		info   WorkerInfo
		taskId int
	})
	//0 for map
	var phase int
	var mapTasks [][]taskStatus = make([][]taskStatus, M)
	var mapOutput [][]string = make([][]string, nReduce)
	var reduceTasks [][]taskStatus = make([][]taskStatus, nReduce)
	c := Coordinator{mTasks: &mapTasks, rTasks: &reduceTasks, mOutput: &mapOutput, mInputs: files}
	c.server(sockname)

	//scheduler
	go func() {
		//local completedMTasks and completedRTasks..one thread have access, no locks
		for {
			availWorkers <- 1
			args := []string{"wc.so", sockname}
			//TODO: handle test cases as test runs in src/mr
			cmd := exec.Command("./mrworker", args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Env = []string{fmt.Sprintf("PHASE=%d", phase), "GOCACHE=/tmp/go-cache"}
			if err := cmd.Start(); err != nil {
				log.Fatal(err)
			}
			if cmd.Process == nil {
				<-availWorkers
				continue
			}
		}
	}()
	go c.workerMonitor()
	return &c
}

// func (c *Coordinator) markTask
// func (c *Coordinator) markTaskProgress
// func (c *Coordinator) markTaskFailed
// func (c *Coordinator) markTaskCompleted
func (c *Coordinator) markTaskFailed(typ int, taskId int, info WorkerInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var tasks *[][]taskStatus
	if typ == 0 {
		tasks = c.mTasks
	} else {
		tasks = c.rTasks
	}
	task := (*tasks)[taskId]
	task = append(task, taskStatus{info: info, state: failed})
	(*tasks)[taskId] = task
}

func (c *Coordinator) workerMonitor() {
	for {
		worker := <-workers
		client, err := connectrpc(worker.info.Sockname)
		if err != nil {
			c.markTaskFailed(worker.info.Type, worker.taskId, worker.info)
			continue
		}
		go func(client *rpc.Client, worker struct {
			info   WorkerInfo
			taskId int
		}) {
			done := make(chan *rpc.Call, 1)
			for {
				timeout := time.After(10 * time.Second)
				if worker.info.Type == 0 {
					counts := MapCounts{}
					client.Go("MapState.Snapshot", worker.taskId, &counts, done)
					select {
					case <-timeout:
						c.markTaskFailed(0, worker.taskId, worker.info)
						<-availWorkers
						return
					//TODO: return when worker task is finished
					case <-done:
						return
					}
				} else {
					counts := ReducerCounts{}
					client.Go("ReducerState.Snapshot", worker.taskId, &counts, done)
					select {
					case <-timeout:
						c.markTaskFailed(1, worker.taskId, worker.info)
						<-availWorkers
						return
					case c := <-done:
						if c.Error != nil {
							log.Fatalf("Shutting down master from workerMonitor on a reduce Task %v\n", c.Error)
							// TODO: c.markTaskFailed
						}
						reply, ok := c.Reply.(*ReducerCounts)
						if !ok {
							//rarely the case
							log.Fatal("Wrong reply type at RPC ReducerState.Snapshot Service\n")
						}
						fmt.Printf("Reduce task %d with worker Id %d produces %d unique intermediate keys with total %d keys processed so far\n", worker.taskId, worker.info.WorkerId, reply.Keys, reply.Values)
						if reply.State == completed {
							completedRTasks <- worker.taskId
							//send 1 to channel
							<-availWorkers // Consumed Workers returning
							fmt.Printf("Reduce task %d with worker Id %d produces %d unique intermediate keys with total %d keys processed so far\n", worker.taskId, worker.info.WorkerId, reply.Keys, reply.Values)
							return
						}
					}
				}
				time.Sleep(1 * time.Second)
			}
		}(client, worker)
	}
}
