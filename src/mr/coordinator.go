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

//TODO: Add separate locks for rTasks and mTasks

var reduceTasksCnt int

// It's use have a similar behavior like a sem
var availWorkers chan int

var completedMTasks int
var completedRTasks int

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
	return fmt.Errorf("All rtasks are inprogress")
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

	// when we hit here should we let the scheduler know ??
	//TODO: change this for backup Tasks (3.6)
	return fmt.Errorf("All tasks are inprogress")
}

// in_progress -> completed
func (c *Coordinator) Notify(arg NotifyTask, reply *ReceivedNotofication) error {
	if reduceTasksCnt != len(arg.ReduceTasks) {
		*reply = -1 //INSUF_RT
		return fmt.Errorf("Expected %d but got %d", reduceTasksCnt, len(arg.ReduceTasks))
	}
	<-availWorkers
	c.mu.Lock()
	defer c.mu.Unlock()

	//TODO: refactor markTaskCompleted
	completedMTasks++
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
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return false
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	fmt.Printf("listening at %s\n", sockname)
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
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	defer func() {
		//remove produced files
	}()

	M := len(files)
	reduceTasksCnt = nReduce
	availWorkers = make(chan int, 4)
	workers = make(chan struct {
		info   WorkerInfo
		taskId int
	})
	//0 for map
	var phase int
	var mapTasks [][]taskStatus = make([][]taskStatus, 0, M)
	var mapOutput [][]string = make([][]string, 0, nReduce)
	var reduceTasks [][]taskStatus = make([][]taskStatus, 0, nReduce)
	c := Coordinator{mTasks: &mapTasks, rTasks: &reduceTasks, mOutput: &mapOutput, mInputs: files}
	c.server(sockname)

	//scheduler
	go func() {
		for {
			availWorkers <- 1
			args := []string{"run", "mrworker.go", "wc.so", sockname}
			//TODO: use binaries
			cmd := exec.Command("/usr/local/go/bin/go", args...)

			cmd.Dir = ""
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if completedMTasks == M {
				fmt.Printf("All map tasks completed, switching to reduce phase\n")
				phase = 1
			}
			cmd.Env = []string{fmt.Sprintf("PHASE=%d", phase), "GOCACHE=/tmp/go-cache"}
			cmd.Start()
			if cmd.Process == nil {
				<-availWorkers
				continue
			}
			fmt.Printf("Started worker with PID %d for phase %d\n", cmd.Process.Pid, phase)
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
			done := make(chan *rpc.Call)
			for {
				timeout := time.After(10 * time.Second)
				if worker.info.Type == 0 {
					counts := MapCounts{}
					client.Go("MapState.Snapshot", TaskId(worker.taskId), &counts, done)
					select {
					case <-timeout:
						c.markTaskFailed(0, worker.taskId, worker.info)
						<-availWorkers
						return
					case <-done:
						{
							fmt.Printf("Received reply message from worker Id %d with task %d", worker.info.WorkerId, worker.taskId)
						}
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
							log.Fatal("Shutting down master")
						}
						reply, ok := c.Reply.(*ReducerCounts)
						if !ok {
							//rarely the case
							log.Fatal("Wrong reply type at RPC ReducerState.Snapshot Service")
						}
						fmt.Printf("Reduce task %d with worker Id %d produces %d unique intermediate keys with total %d keys processed so far", worker.taskId, worker.info.WorkerId, reply.Keys, reply.Values)
						if reply.State == completed {
							<-availWorkers
						}
					}
				}
				time.Sleep(1 * time.Second)
			}
		}(client, worker)
	}
}
